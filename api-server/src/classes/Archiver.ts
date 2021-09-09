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

type IStreamService = 'odysee' | 'bitwave';

export interface IArchiveTransmuxed {
  file: string;
  key: string;
  type: 'flv' | 'mp4';
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

  async handleReplayTransmuxing(user: string, recordName: string, fileLocation: string): Promise<boolean> {
    return new Promise<boolean>((async (resolve, reject) => {

      const fileName = path.basename(fileLocation);

      //check size/length constraints
      let stats = fs.statSync(fileLocation);
      let fileSizeInBytes = stats.size;
      if (fileSizeInBytes > 6 * 1024 * 1024 * 1024) {
        console.error(`[archiver] ${fileName} is too big to process (${fileSizeInBytes / 1024 / 1024 / 1024}GB)`);
        resolve(false)
        return;
      }
      if (fileSizeInBytes < 10 * 1024 * 1024) {
        console.error(`[archiver] ${fileName} is too small to process (${fileSizeInBytes / 1024}KB)`);
        resolve(false)
        return;
      }
      try {
        let videoInfo = await this.probeVideo(fileLocation)
        if (videoInfo.format.duration > 6 * 60 * 60) {
          console.error(`[archiver] ${fileName} is too long to process (${videoInfo.format.duration * 60 * 60} Hours)`);
          resolve(false)
          return;
        }
        if (videoInfo.format.duration < 30) {
          console.error(`[archiver] ${fileName} is too short to process (${videoInfo.format.duration} Seconds)`);
          resolve(false)
          return;
        }
      } catch (e) {
        console.error(`[archiver] ${fileName} failed to probe: ${e}`);
        reject(`[archiver] ${fileName} failed to probe: ${e}`)
        return;
      }

      // Transfer FLV file to replay transcoder
      console.log(`[archiver] ${fileName} transferring replay to the transcoder Server...`);
      try {
        await this.transferArchive(fileLocation, fileName);
      } catch (e) {
        console.error(`[archiver] ${fileName} failed to transfer to transcoder server: ${e}`);
        resolve(true)
        return;
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

    let shouldRetry = true;
    for (let i = 0; i < 3 && shouldRetry; i++) {
      try {
        shouldRetry = await this.handleReplayTransmuxing(user, recordName, fileLocation)
      } catch (e) {
        shouldRetry = true;
        break
      }
    }
    if (shouldRetry) {
      console.error(`[archiver] ${fileName} replay failed to process. Giving up (but retaining the file)`)
      try {
        await fsPromises.rename(fileLocation, `/archives/rec/failed/${fileName}`)
      } catch (e) {
        console.error(`[archiver] ${fileName} failed to move to failed directory: ${e}`)
      }
      return
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

  async notifyTranscodeServer(filename: string, fileLocation: string, channelId: string): Promise<boolean> {
    return new Promise<boolean>(async (resolve, reject) => {
      try {
        const fileBuffer = fs.readFileSync(fileLocation);
        const hashSum = crypto.createHash('sha256');
        hashSum.update(fileBuffer);
        const hex = hashSum.digest('hex');

        const options = {
          resolveWithFullResponse: true,
          form: {
            file_name: filename,
            channel_id: channelId,
            secret: 'TODO-USE-SOME-ENV-VAR', // TODO: use an env var here
            sha256: hex,
          },
        };

        const response = await rp.post('https://transcoder.live.odysee.com/stream', options)

        if (response.statusCode >= 300) {
          if (response.statusCode == 470) { // we're using this code to mean the upload to the server didn't work and we should try again
            resolve(false)
          } else {
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
          resolve(true);
          return
        }

      } catch (error) {
        console.error(error.message);
        reject(error);
      }
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
