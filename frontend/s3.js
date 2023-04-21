import axios from "axios";

/**
 * Uploads a file to S3 via MultiPart upload splitting the files into chunks.
 * @param {string} path The path inside the S3 bucket to store the file in
 * @param {File} file The file that needs to be splitted into parts
 * @param {Function} uploadProgressFn The function to update the progress on the UI
 * @param {number} chunkSize The desired size for the parts, defaults to 100 MB
 * @returns 
 */
export async function uploadMultipart(path, file, chunkSize = 100 * 1024 * 1024) {
    
    const fileName = `${path}${file.name}`
    // Start multipart upload
    const uploadId = await startMultipartUpload(fileName);
    let bytesTransferred = 0;
    let progress = 0;
    try {
        console.log(file);
        const fileSize = file.size;
        const NUM_CHUNKS = Math.floor(fileSize / chunkSize) + 1;
        let promiseArray = [];
        let start, end, blob;


        for(let index = 1; index < NUM_CHUNKS + 1; index++) {
            // Make blob with chunk size
            start = (index - 1) * chunkSize;
            end = index * chunkSize;
            blob = index < NUM_CHUNKS ? file.slice(start, end) : file.slice(start);

            // Get presigned URL for each part
            let partSignedUrl = await preSignPart(uploadId, fileName, index);

            // Put part to the storage server with the presigned url
            let uploadResponse = axios.put(
                partSignedUrl,
                blob,
                {
                    headers: {
                        'Content-Type': file.type,
                        'Access-Control-Expose-Headers': 'ETag'
                    },
                    onUploadProgress: function(progressEvent) {
                        bytesTransferred += progressEvent.bytes;
                        progress = bytesTransferred / file.size;
                        console.log(progress);
                    }
                }
            ).catch(
                (err) => console.error(err)
            )
            promiseArray.push(uploadResponse);
        }

        let resolvedArray = await Promise.all(promiseArray);

        let uploadPartsArray = [];
        resolvedArray.forEach((resolvedPromise, index) => {
            uploadPartsArray.push({
                ETag: resolvedPromise.headers.etag,
                PartNumber: index + 1
            });
        })

        // Complete MultiPart upload
        const response = await completeMultipartUpload(uploadId, fileName, uploadPartsArray);
        return response
    } catch (err) {
        console.error(err)
        const response = await abortMultipartUpload(uploadId, fileName);
    }
}

/**
 * Sends a request to the backend server to start a MultiPart upload.
 * @param {string} fileName The path to the file inside the S3 bucket
 * @returns {string} The correspondent upload ID of the created MultiPart upload
 */
async function startMultipartUpload(fileName) {
    let response = await axios.get(
        'http://localhost:9999/uploads/start',
        {
            params: {
                fileName: fileName,
            }
        });

    return response.data
}

/**
 * Send a request to the backend server to obtain a presigned url for a certain part for MultiPart upload.
 * @param {string} uploadId The correspondent upload ID of the created MultiPart upload
 * @param {string} fileName The path to the file inside the S3 bucket
 * @param {number} partNumber The number of the part being uploaded
 * @returns 
 */
async function preSignPart(uploadId, fileName, partNumber, ) {
    let response = await axios.get(
        'http://localhost:9999/uploads/sign/part',
        {
            params: {
                fileName: fileName,
                partNumber: partNumber,
                uploadId: uploadId,
            }
        });

    return response.data
}

/**
 * Sends a request to the backend server stating that every part has been succesfully uploaded.
 * @param {string} uploadId The upload identifier for this MultiPart upload
 * @param {string} fileName The path to the file inside the S3 bucket
 * @param {[{"ETag": str, "PartNumber": number}]} parts An array containing the information of each of the uploaded parts
 * @returns 
 */
async function completeMultipartUpload(uploadId, fileName, parts) {
    let response = await axios.post(
        'http://localhost:9999/uploads/complete',
        parts,
        {
            params: {
                uploadId: uploadId,
                fileName: fileName,
            }
        });

    return response.data
}

/**
 * Send a request to the backend server informing that there has been an error uploading any of the parts and the MultiPart upload must be aborted in order to free up resources from the S3 bucket.
 * @param {string} uploadId The upload identifier for this MultiPart upload
 * @param {string} fileName The path to the file inside the S3 bucket
 * @returns 
 */
async function abortMultipartUpload(uploadId, fileName) {
    let response = await axios.post(
        'http://localhost:9999/uploads/abort',
        null,
        {
            params: {
                uploadId: uploadId,
                fileName: fileName,
            }
        });

    return response
}
