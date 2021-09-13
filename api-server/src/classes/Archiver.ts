// Created by xander on 12/30/2019

import * as admin from 'firebase-admin';
import { promises as fsp, readFileSync } from 'fs';
import * as path from 'path';
import * as chalk from 'chalk';
import * as FfmpegCommand from 'fluent-ffmpeg';
import { ffprobe, FfprobeData, FfprobeStream } from 'fluent-ffmpeg';

import { stackpaths3 } from '../services/s3Storage';
import * as rp from 'request-promise';
import Client from 'node-scp';

const transcoderPrivateKey = readFileSync('../../creds/ssh-key.ppk');
const transcodeServer = 'transcoder.live.odysee.com';
const transcoderUser = 'lbry';

type IStreamService = 'odysee' | 'bitwave';

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

class ArchiveManager {
  public recorders: IRecorder[];

  constructor () {
    this.recorders = [];
  }

  async startArchive ( user: string, recordName: string ) {
    const id = `${user}-${recordName}`;
    const inputStream  = `rtmp://nginx-server/live/${user}`;
    const outputFile = `/archives/rec/${user}_${recordName}_${Date.now()}.flv`

    // Check for existing recorder with same user ane name
    const recorders = this.recorders.find( t => t.id.toLowerCase() === id.toLowerCase() );
    if ( recorders && recorders.process !== null ) {
      console.log( `${id} is already being recorded.` );
      return;
    }

    console.log( `starting recording: ${id}` );

    return new Promise<string>( ( res ) => {
      // Create Command
      const ffmpeg = FfmpegCommand({ stdoutLines: 3 });

      ffmpeg.input( inputStream );
      ffmpeg.inputOptions([
        '-err_detect ignore_err',
        '-ignore_unknown',
        '-fflags nobuffer+genpts+igndts',
      ]);

      ffmpeg.output( outputFile );
      ffmpeg.outputOptions([
        '-c copy',
      ]);

      // Event handlers
      ffmpeg
        .on( 'start', commandLine => {
          console.log( chalk.greenBright ( `[${recordName}] Started recording stream: ${user}` ) );
          console.log( commandLine );
          res( outputFile );
        })

        .on( 'end', () => {
          console.log( chalk.greenBright ( `[${recordName}] Ended stream recording for: ${user}` ) );
          this.recorders = this.recorders.filter( t => t.id.toLowerCase() !== id.toLowerCase() );
          this.onArchiveEnd( user, recordName, outputFile );
        })

        .on( 'error', ( error, stdout, stderr ) => {
          console.log( error );
          console.log( stdout );
          console.log( stderr );

          if ( error.message.includes('SIGKILL') ) {
            console.error( chalk.redBright ( `${user}: Stream recording stopped!` ) );
          } else {
            console.error( chalk.redBright ( `${user}: Stream recording error!` ) );
          }

          this.recorders = this.recorders.filter( t => t.id.toLowerCase() !== id.toLowerCase() );
        })

      // Start
      ffmpeg.run();
    });
  }

  async stopArchive ( user: string, recordName: string ) {
    const id = `${user}-${recordName}`;
    const recorders = this.recorders.find( t => t.id.toLowerCase() === id.toLowerCase() );
    if ( recorders.process !== null ) {
      recorders.process.kill( 'SIGKILL' );
      console.log( `Stopping recording for: ${id}` );
      return true;
    } else {
      console.log( `Not recording: ${id}` )
      return false;
    }
  }

  async onArchiveEnd ( user: string, recordName: string, fileLocation: string ): Promise<void> {
    // Log debug info
    console.log( `[${recordName}] Replay for ${user} saved to ${fileLocation}.` );

    // Extract filename from file location
    // path.parse(fileLocation).base;
    const fileName = path.basename( fileLocation );

    // Transfer FLV file to replay transcoder
    console.log( `[${fileName}] Transferring FLV to Replay Transcode Server...` );

    await this.transferArchive( fileLocation, fileName );

    // Notify transcoding server of new replay
    console.log( `[${fileName}] Notifying Replay Transcode Server of new replay...` );
    await this.notifyTranscodeServer( fileName, user );

    // Delete FLV file after successful transmux on transcoding server
    console.log( `[${fileLocation}] will now be deleted...` );
    await this.deleteFLV( fileLocation );

    console.log( `[${fileName}] Replay processing compete!` );

    return;


    // --------------------------------------------
    // --- USED TO TRANSMUX ON INGESTION SERVER ---
    // --------------------------------------------

    console.log( `[${recordName}] Converting FLV to MP4 and generating thumbnails...` );

    // Transmux file
    const result: IArchiveTransmuxed = await this.transmuxArchive( fileLocation, user, recordName );

    // Log debug info (replay duration)
    const rMin: number = Math.trunc( result.duration / 60 );
    const rSec: number = Math.trunc( result.duration ) % 60;
    console.log( `[${recordName}] Replay is ${rMin}:${rSec}` );

    // disable processing replays longer than 6 hours
    if ( rMin > 360 ) {
      console.log(`Replay is too long to process: ${fileLocation}`);
      return;
    }

    // use recordName to detect which platform's service was used
    const service: IStreamService = recordName === 'odysee'? 'odysee' : 'bitwave';

    // API call when completed
    try {
      const options = {
        form: {
          channel: user,
          service: service,
          result: result,
        },
      };
      await rp.post( 'http://api-server:3000/archive/end', options );
    } catch ( error ) {
      console.error( error.message );
    }
  }

  async deleteArchive ( archiveId: string ) {
    try {
      // Create db reference to archive
      const archiveReference = admin.firestore()
        .collection( 'archives' )
        .doc( archiveId );

      const archiveDocument = await archiveReference.get();

      // Get data from archive
      const archive = archiveDocument.data();

      // Delete archive file
      await fsp.unlink( archive.file );
      console.log( `${archive._username}'s archive deleted: ${archiveId}` );

      // Flag archive as deleted
      await archiveReference
        .update( { deleted: true } );

      // Return results
      return {
        success: true,
        message: `archive deleted: ${archiveId}`,
      };

    } catch ( error ) {
      // An error occurred while attempting to delete an archive
      console.log( error );
      return {
        success: false,
        message: error.message,
      };

    }
  }

  async transmuxArchive ( file: string, channel: string, recordName: string ): Promise<IArchiveTransmuxed> {
    const transmuxAsync = ( file: string ): Promise<string> => new Promise( ( res, reject ) => {
      // Change flv to mp4
      const outFile = file.replace( /\.flv$/i, '.mp4' );

      const ffmpeg = FfmpegCommand();
      ffmpeg.input( file );
      ffmpeg.inputOptions([
        '-err_detect ignore_err',
        '-ignore_unknown',
        '-stats',
      ]);

      ffmpeg.output( outFile );
      ffmpeg.outputOptions([
        '-codec:a copy', // Audio (copy)
        '-codec:v copy', // Video (copy)

        // Video (transcode)
        // '-c:v libx264',
        // '-preset:v superfast', // preset
        // '-crf 35', // Quality level

      ]);

      ffmpeg.renice( 5 );

      ffmpeg
        .on('start', command => {
          console.log( chalk.greenBright(`Starting archive transmux.`) );
        })

        .on( 'progress', progress => {
          console.log( progress );
        })

        .on('end', ( stdout, stderr ) => {
          console.log( chalk.greenBright(`Finished archive transmux.`) );
          return res ( outFile );
        })

        .on( 'error', ( error, stdout, stderr ) => {
          console.log( chalk.redBright(`Error during archive transmux.`) );

          console.log( error );
          console.log( stdout );
          console.log( stderr );

          return reject( error );
        });

      ffmpeg.run();
    });

    const probeTransmuxedFile = ( file: string ): Promise<object> => new Promise ( ( res, reject ) => {
      ffprobe( file, ( error, data: FfprobeData ) => {
        if ( error ) return reject( error );

        // Video Data
        const videoData = data.streams.find(stream => stream.codec_type === 'video' );
        if ( videoData ) {
          // console.log( videoData );
        }

        // Audio Data
        const audioData = data.streams.find(stream => stream.codec_type === 'audio' );
        if ( audioData ) {
          // console.log( audioData );
        }

        return res({ video: videoData, audio: audioData, format: data.format });
      });
    });

    const generateScreenshots = ( file: string, screenshots: number ): Promise<string[]> => new Promise ( async (res, reject) => {

      const takeScreenshots = ( file, count ): Promise<string[]> => {
        const folder = path.dirname( file );
        // Take single screenshot, hopefully with the seek ffmpeg command
        const takeSingleScreenshot = async ( file, timestamp, index ) => {
          return await new Promise( ( res, reject ) => {
            let filename = null;
            const ffmpeg = FfmpegCommand;
            ffmpeg(file)
              .renice( 5 )

              .on("start", ( command ) => {
                console.log(`[START] taking screenshot: ${index} at ${timestamp}`);
                console.log( `[START] Screenshot command:`, command );
              })

              .on("end", () => {
                console.log(`[END] screenshot #${index} at: ${timestamp} complete.`);
                if ( filename ){
                  return res( filename );
                } else {
                  console.error( `Missing screenshot filename!` );
                  res( '' );
                }
              })

              .screenshots({
                count: 1,
                timemarks: [timestamp],
                filename: `%b_${index}.jpg`,
                folder: folder,
              })

              .on('filenames', ( outputFilenames: string[] ) => {
                console.log( `[FILES] Took screenshot:`, outputFilenames );
                const filenames = outputFilenames.map( f => `${folder}/${f}` );
                filename = filenames[0];
              })

              .on( 'error', ( error, stdout, stderr ) => {
                console.log( chalk.redBright(`[ERROR] Error generating screenshots.`) );

                console.log( error );
                console.log( stdout );
                console.log( stderr );

                return reject( error );
              });
          });
        }

        // const count = 10;
        const timestamps = [];
        const startPositionPercent = 5;
        const endPositionPercent = 95;
        const addPercent = ( endPositionPercent - startPositionPercent ) / ( count - 1 );

        for ( let i = 0; i < count; i++ ) {
          const time = startPositionPercent + addPercent * i;
          timestamps.push( `${time}%` );
        }

        return new Promise ( async ( res, reject ) => {
          const files = [];
          await Promise.all (
            timestamps.map( async (timestamp, index) => {
              const screenshotFile = await takeSingleScreenshot( file, timestamp, index );
              files.push( screenshotFile );
            })
          );
          res( files );
        });
      }

      const screenshotFiles = await takeScreenshots( file, screenshots );
      console.log( `Screenshots finished!\n`, screenshotFiles );

      res( screenshotFiles );
    });

    // use recordName to detect which platform's service was used
    const service: IStreamService = recordName === 'odysee'? 'odysee' : 'bitwave';

    // Transmux FLV -> mp4
    let transmuxFile: string = null;
    try {
      transmuxFile = await transmuxAsync( file );
    } catch ( error ) {
      console.log( chalk.redBright( `Archive transmux failed... Bailing early.` ) );
      console.log( error );
      return {
        file: file,
        key: file,
        type: 'flv',
        duration: 0,
        thumbnails: [],
        channel: channel,
        service: service,
        fileSize: 0,
        ffprobe: {
          videoData: [],
          audioData: [],
        },
      };
    }

    if ( !transmuxFile ) {
      console.log( chalk.redBright( `Archive transmux failed... Bailing early.` ) );
      return {
        file: file,
        key: file,
        type: 'flv',
        duration: 0,
        thumbnails: [],
        channel: channel,
        service: service,
        fileSize: 0,
        ffprobe: {
          videoData: [],
          audioData: [],
        },
      };
    }


    // Probe resulting mp4
    let transmuxData = null;
    try {
      transmuxData = await probeTransmuxedFile( transmuxFile );
    } catch ( error ) {
      console.log( chalk.redBright( `Archive transmux probe failed... Bailing early.` ) );
      console.log( error );
      return {
        file: file,
        key: file,
        type: 'flv',
        duration: 0,
        thumbnails: [],
        channel: channel,
        service: service,
        fileSize: 0,
        ffprobe: {
          videoData: [],
          audioData: [],
        },
      };
    }

    if ( !transmuxData ) {
      console.log( `Archive transmux probe failed... Bailing early.` );
      return {
        file: file,
        key: file,
        type: 'flv',
        duration: 0,
        thumbnails: [],
        channel: channel,
        service: service,
        fileSize: 0,
        ffprobe: {
          videoData: [],
          audioData: [],
        },
      };
    }


    // Generate screenshots from mp4
    let thumbnails = [];
    try {
      thumbnails = await generateScreenshots( transmuxFile, 10 );
    } catch ( error ) {
      console.log( chalk.redBright( `Thumbnail generation failed!` ) );
      console.log( error );
      thumbnails = [];
    }


    console.log( `Delete source FLV file...` );

    // Delete source FLV file
    try {
      await fsp.unlink( file );
      console.log( chalk.greenBright( `${file} deleted.` ) );
    } catch ( error ) {
      console.log( chalk.redBright( `Archive source flv delete failed... This is bad..` ) );
      console.log( error );
    }



    // S3 Debug
    console.log( `Get S3 debug info...` );
    await stackpaths3.listBuckets();


    // S3 Upload thumbnails
    let s3Thumbnails: string[] = [];
    if ( thumbnails && thumbnails.length > 0 ) {
      console.log( `Uploading thumbnails to S3 bucket...` );

      // Upload thumbnails to S3
      try {
        s3Thumbnails = await Promise.all(
          thumbnails.map( async thumbnail => {
            return (await stackpaths3.uploadImage( thumbnail, service )).location;
          })
        );
      } catch ( error ) {
        console.log( chalk.redBright( `Thumbnail upload failed... This is probably bad..` ) );
        console.log( error );
      }

      // Delete local thumbnail files
      console.log( `Delete thumbnails on local server...` );

      // Delete thumbnails
      try {
        await Promise.all(
          thumbnails.map( async thumbnail => {
            await fsp.unlink( thumbnail );
            console.log( chalk.greenBright( `${thumbnail} deleted.` ) );
          })
        );
      } catch ( error ) {
        console.log( chalk.redBright( `Thumbnail delete failed... This is probably bad..` ) );
        console.log( error );
      }
    } else {
      s3Thumbnails = null;
    }


    // S3 Upload video
    console.log( `Upload mp4 to S3 bucket...` );
    const s3File = await stackpaths3.upload( transmuxFile, service );

    // Delete local mp4 file
    console.log( `Delete transmuxed mp4 file on local server...` );

    // Delete source mp4 file
    try {
      await fsp.unlink( transmuxFile );
      console.log( chalk.greenBright( `${transmuxFile} deleted.` ) );
    } catch ( error ) {
      console.log( chalk.redBright( `Archive source mp4 delete failed... This is bad..` ) );
      console.log( error );
    }


    // Finished processing!
    return {
      file: s3File.location,
      key: s3File.key,
      type: 'mp4',
      duration: transmuxData.video.duration,
      thumbnails: s3Thumbnails,
      channel: channel,
      service: service,
      fileSize: transmuxData.format.size,
      ffprobe: {
        videoData: transmuxData.video,
        audioData: transmuxData.audio,
      },
    };
  }

  async transferArchive ( file: string, fileName: string ) {
    try {
      // Connect to other server
      const scpClient = await Client({
        host: transcodeServer,
        port: 22,
        username: transcoderUser,
        privateKey: transcoderPrivateKey,
      });

      // Transfer file via SCP
      await scpClient.uploadFile( file, `videos_to_transcode/${fileName}` );

      // Close connection
      scpClient.close();
    } catch (error) {
      console.error(error.message);
    }
  }

  async notifyTranscodeServer ( filename: string, channelId: string ) {
    try {
      const options = {
        form: {
          file_name: filename,
          channel_id: channelId,
          secret: 'TODO-USE-SOME-ENV-VAR', // TODO: use an env var here
        },
      };
      await rp.post( 'https://transcoder.live.odysee.com/stream', options );
    } catch ( error ) {
      console.error( error.message );
    }
  }

  async deleteFLV ( file: string ) {
    // Delete source FLV file
    try {
      await fsp.unlink( file );
      console.log( chalk.greenBright( `${file} deleted.` ) );
    } catch ( error ) {
      console.log( chalk.redBright( `${file}: Replay source flv delete failed... This is bad..` ) );
      console.log( error );
    }
  }
}

export const archiver = new ArchiveManager();
