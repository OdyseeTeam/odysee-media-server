// Created by xander on 12/30/2019

import * as admin from 'firebase-admin';
import {promises as fsp, readFileSync} from 'fs';
import * as path from 'path';
import * as chalk from 'chalk';
import * as FfmpegCommand from 'fluent-ffmpeg';
import {ffprobe, FfprobeData, FfprobeStream} from 'fluent-ffmpeg';

import * as rp from 'request-promise';
import Client from 'node-scp';

const transcoderPrivateKey = readFileSync('../../creds/ssh-key.ppk');
const transcodeServer = 'transcoder.live.odysee.com';
const transcoderUser = 'lbry';

const crypto = require('crypto');
const fs = require('fs');
const fsPromises = fs.promises;

type IStreamService = 'odysee'|'bitwave';

export interface IArchiveTransmuxed {
  file: string;
  key: string;
  type: 'flv'|'mp4';
  duration: number;
  fileSize: number;
  thumbnails: string[];
  channel: string;
  service: IStreamService;
  ffprobe: {
    videoData: FfprobeStream[],
    audioData: FfprobeStream[],
  };
}

interface IRecorder {
  id: string;
  process: any;
}

enum RetryState {
  RETRY,
  RETRY_NOUPLOAD,
  NORETRY_NODELETE,
  NORETRY_DELETE,
}

class ArchiveManager {
  public recorders: IRecorder[];

  constructor() {
    this.recorders = [];
  }

  async startArchive(user: string, recordName: string) {
    const id = `${user}-${recordName}`;
    const inputStream = `rtmp://nginx-server/live/${user}`;
    const outputFile = `/archives/rec/${user}_${recordName}_${Date.now()}.flv`

    // Check for existing recorder with same user ane name
    const recorders = this.recorders.find(t => t.id.toLowerCase() === id.toLowerCase());
    if (recorders && recorders.process !== null) {
      console.log(`${id} is already being recorded.`);
      return;
    }

    console.log(`starting recording: ${id}`);

    return new Promise<string>((res) => {
      // Create Command
      const ffmpeg = FfmpegCommand({stdoutLines: 3});

      ffmpeg.input(inputStream);
      ffmpeg.inputOptions([
        '-err_detect ignore_err',
        '-ignore_unknown',
        '-fflags nobuffer+genpts+igndts',
      ]);

      ffmpeg.output(outputFile);
      ffmpeg.outputOptions([
        '-c copy',
      ]);

      // Event handlers
      ffmpeg
        .on('start', commandLine => {
          console.log(chalk.greenBright(`[${recordName}] Started recording stream: ${user}`));
          console.log(commandLine);
          res(outputFile);
        })

        .on('end', () => {
          console.log(chalk.greenBright(`[${recordName}] Ended stream recording for: ${user}`));
          this.recorders = this.recorders.filter(t => t.id.toLowerCase() !== id.toLowerCase());
          this.onArchiveEnd(user, recordName, outputFile);
        })

        .on('error', (error, stdout, stderr) => {
          console.log(error);
          console.log(stdout);
          console.log(stderr);

          if (error.message.includes('SIGKILL')) {
            console.error(chalk.redBright(`${user}: Stream recording stopped!`));
          } else {
            console.error(chalk.redBright(`${user}: Stream recording error!`));
          }

          this.recorders = this.recorders.filter(t => t.id.toLowerCase() !== id.toLowerCase());
        })

      // Start
      ffmpeg.run();
    });
  }

  async stopArchive(user: string, recordName: string) {
    const id = `${user}-${recordName}`;
    const recorders = this.recorders.find(t => t.id.toLowerCase() === id.toLowerCase());
    if (recorders.process !== null) {
      recorders.process.kill('SIGKILL');
      console.log(`Stopping recording for: ${id}`);
      return true;
    } else {
      console.log(`Not recording: ${id}`)
      return false;
    }
  }

  async probeVideo(fileLocation: string): Promise<FfprobeData> {
    return new Promise<FfprobeData>((res, reject) => {
      ffprobe(fileLocation, (error, data: FfprobeData) => {
        if (error) return reject(error);

        return res(data);
      });
    })
  }

  async handleReplayTransmuxing(user: string, recordName: string, fileLocation: string, shouldUpload: boolean): Promise<RetryState> {
    return new Promise<RetryState>((async (resolve, reject) => {

      const fileName = path.basename(fileLocation);

      //check size/length constraints
      let stats = fs.statSync(fileLocation);
      let fileSizeInBytes = stats.size;
      if (fileSizeInBytes > 6 * 1024 * 1024 * 1024) {
        console.error(`[archiver] ${fileName} is too big to process (${fileSizeInBytes / 1024 / 1024 / 1024}GB)`);
        resolve(RetryState.NORETRY_DELETE);
        return;
      }
      if (fileSizeInBytes < 10 * 1024 * 1024) {
        console.error(`[archiver] ${fileName} is too small to process (${fileSizeInBytes / 1024}KB)`);
        resolve(RetryState.NORETRY_DELETE);
        return;
      }
      try {
        let videoInfo = await this.probeVideo(fileLocation)
        if (videoInfo.format.duration > 6 * 60 * 60) {
          console.error(`[archiver] ${fileName} is too long to process (${videoInfo.format.duration * 60 * 60} Hours)`);
          resolve(RetryState.NORETRY_DELETE);
          return;
        }
        if (videoInfo.format.duration < 30) {
          console.error(`[archiver] ${fileName} is too short to process (${videoInfo.format.duration} Seconds)`);
          resolve(RetryState.NORETRY_DELETE);
          return;
        }
      } catch (e) {
        console.error(`[archiver] ${fileName} failed to probe: ${e}`);
        reject(`[archiver] ${fileName} failed to probe: ${e}`)
        return;
      }

      // Transfer FLV file to replay transcoder
      if (shouldUpload) {
        console.log(`[archiver] ${fileName} transferring replay to the transcoder Server...`);
        try {
          await this.transferArchive(fileLocation, fileName);
        } catch (e) {
          console.error(`[archiver] ${fileName} failed to transfer to transcoder server: ${e}`);
          resolve(RetryState.RETRY);
          return;
        }
      }

      // Notify transcoding server of new replay
      console.log(`[archiver] ${fileName} notifying transcoder server of the new replay...`);
      try {
        let shouldRetry = await this.notifyTranscodeServer(fileName, fileLocation, user);
        resolve(shouldRetry)
      } catch (e) {
        console.error(`[archiver] ${fileName} transcoder server failed to process the replay: ${e}`);
        reject(`[archiver] ${fileName} transcoder server failed to process the replay: ${e}`)
        return;
      }
    }))
  }

  async onArchiveEnd(user: string, recordName: string, fileLocation: string) {
    const fileName = path.basename(fileLocation);
    console.log(`[${recordName}] Replay for ${user} saved to ${fileLocation}.`);

    let state = RetryState.RETRY;
    let retries = 0;
    while (state === RetryState.RETRY || state === RetryState.RETRY_NOUPLOAD) {
      if (retries >= 3) {
        console.log(`[archiver] ${fileName} maximum number of retries reached (3). Giving up.`);
        break;
      }
      try {
        state = await this.handleReplayTransmuxing(user, recordName, fileLocation, state === RetryState.RETRY)
      } catch (e) {
        console.error(`[archiver] ${fileName} failed to process a replay for unexpected reasons: ${e}`);
        state = RetryState.RETRY;
      }
      retries++;
    }

    switch (state) {
      case RetryState.RETRY_NOUPLOAD:
      case RetryState.RETRY:
      case RetryState.NORETRY_NODELETE:
        console.error(`[archiver] ${fileName} replay failed to process. Giving up (but retaining the file)`);
        try {
          await fsPromises.rename(fileLocation, `/archives/rec/failed/${fileName}`);
        } catch (e) {
          console.error(`[archiver] ${fileName} failed to move to failed directory: ${e}`);
        }
        return;
      //this is sort of the default state, it might indicate successes or hard failures that we don't want to ever retry
      case RetryState.NORETRY_DELETE:
        break;
    }


    // Delete FLV file after successful transmux on the transcoder server
    console.log(`[archiver] ${fileName} will now be deleted...`);
    await this.deleteFLV(fileLocation);

    console.log(`[archiver] ${fileName} replay processing compete!`);
    return;
  }

  async deleteArchive(archiveId: string) {
    try {
      // Create db reference to archive
      const archiveReference = admin.firestore()
        .collection('archives')
        .doc(archiveId);

      const archiveDocument = await archiveReference.get();

      // Get data from archive
      const archive = archiveDocument.data();

      // Delete archive file
      await fsp.unlink(archive.file);
      console.log(`${archive._username}'s archive deleted: ${archiveId}`);

      // Flag archive as deleted
      await archiveReference
        .update({deleted: true});

      // Return results
      return {
        success: true,
        message: `archive deleted: ${archiveId}`,
      };

    } catch (error) {
      // An error occurred while attempting to delete an archive
      console.log(error);
      return {
        success: false,
        message: error.message,
      };
    }
  }

  async transferArchive(file: string, fileName: string): Promise<boolean> {
    return new Promise<boolean>(async (resolve, reject) => {
      try {
        // Connect to other server
        const scpClient = await Client({
          host: transcodeServer,
          port: 22,
          username: transcoderUser,
          privateKey: transcoderPrivateKey,
        });

        // Transfer file via SCP
        await scpClient.uploadFile(file, `videos_to_transcode/${fileName}`);

        // Close connection
        scpClient.close();
        resolve(true);
        return;
      } catch (error) {
        reject(error);
        return;
      }
    })
  }


  async notifyTranscodeServer(filename: string, fileLocation: string, channelId: string): Promise<RetryState> {
    return new Promise<RetryState>(async (resolve, reject) => {

      let hash = crypto.createHash('sha256'),
        stream = fs.createReadStream(fileLocation);

      stream.on('data', _buff => {
        hash.update(_buff, 'utf8');
      });
      stream.on('end', async () => {
        const hex = hash.digest('hex');
        const options = {
          resolveWithFullResponse: true,
          form: {
            file_name: filename,
            channel_id: channelId,
            secret: 'TODO-USE-SOME-ENV-VAR', // TODO: use an env var here
            sha256: hex,
          },
        };
        try {
          const response = await rp.post('https://transcoder.live.odysee.com/stream', options)

          /*
          470 - retry with upload
          471 - retry without re-uploading
          472 - do not retry and do not discard video
          473 - do not retry and discard video
           */
          if (response.statusCode >= 300) {
            switch (response.statusCode) {
              case 470:
                resolve(RetryState.RETRY);
                break;
              case 471:
                resolve(RetryState.RETRY_NOUPLOAD);
                break;
              case 472:
                resolve(RetryState.NORETRY_NODELETE);
                break;
              case 473:
                resolve(RetryState.NORETRY_DELETE);
                break;
              default:
                try {
                  const parsed = JSON.parse(response.body)
                  parsed.statusCode = response.statusCode
                  reject(parsed)
                  return
                } catch (error) {
                  reject({
                    statusCode: response.statusCode,
                    body: response.body
                  })
                  return
                }
            }
          } else {
            resolve(RetryState.NORETRY_DELETE);
            return
          }
        } catch (error) {
          console.error(error.message);
          reject(error);
        }
      });
    })
  }

  async deleteFLV(file: string) {
    // Delete source FLV file
    try {
      await fsp.unlink(file);
      console.log(chalk.greenBright(`${file} deleted.`));
    } catch (error) {
      console.log(chalk.redBright(`${file}: Replay source flv delete failed... This is bad..`));
      console.log(error);
    }
  }
}

export const archiver = new ArchiveManager();
