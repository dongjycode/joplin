const { basicDelta } = require('./file-api');
const { basename } = require('./path-utils');
const shim = require('./shim').default;
const JoplinError = require('./JoplinError').default;
const { Buffer } = require('buffer');
const parser = require('fast-xml-parser');

const S3_MAX_DELETES = 1000;



class FileApiDriverMinio {
	constructor(api, bucket) {
		this.bucket_ = bucket;
		this.api_ = api;
	}

	api() {
		return this.api_;
	}

	requestRepeatCount() {
		return 3;
	}

	makePath_(path) {
		if (!path) return '';
		return path;
	}

	hasErrorCode_(error, errorCode) {
		if (!error) return false;

		if (error.name) {
			return error.name.indexOf(errorCode) >= 0;
		} else if (error.code) {
			return error.code.indexOf(errorCode) >= 0;
		} else if (error.Code) {
			return error.Code.indexOf(errorCode) >= 0;
		} else {
			return false;
		}
	}

	// Because of the way AWS-SDK-v3 works for getting data from a bucket we will
	// use a pre-signed URL to avoid https://github.com/aws/aws-sdk-js-v3/issues/1877
	async presignedUrl(key) {
		return new Promise((resolve, reject) => {
			this.api().presignedUrl('GET', this.bucket_, key, 3600, (error, response) => {
				if (error) reject(error);
				else resolve(response);
			});
		});
	}


	async listObjects(key) {
		return await new Promise((resolve, reject) => {
			const objectsListTemp = [];
			const stream = this.api().listObjectsV2(this.bucket_, key, true, '');

			stream.on('data', obj => objectsListTemp.push(obj.name));
			stream.on('error', reject);
			stream.on('end', () => {
				resolve(objectsListTemp);
			});
		});
	}

	async putObject(key, body) {
		return new Promise((resolve, reject) => {
			this.api().putObject(this.bucket_, key, body, (error, response) => {
				if (error) reject(error);
				else resolve(response);
			});
		});
	}

	async uploadFileFrom(path, key) {
		if (!shim.fsDriver().exists(path)) throw new Error('s3UploadFileFrom: file does not exist');
		const body = await shim.fsDriver().readFile(path, 'base64');
		return new Promise((resolve, reject) => {
			this.api().putObject(this.bucket_, key, Buffer.from(body, 'base64'), (error, response) => {
				if (error) reject(error);
				else resolve(response);
			});
		});
	}

	async removeObject(key) {
		return new Promise((resolve, reject) => {
			this.api().removeObject(this.bucket_, key,
				(error, response) => {
					if (error) {
						console.error(error);
						reject(error);
					} else { resolve(response); }
				});
		});
	}

	// Assumes key is formatted, like `{Key: 's3 path'}`
	async removeObjects(keys) {
		return new Promise((resolve, reject) => {
			this.api().removeObjects(this.bucket_, keys,
				(error, response) => {
					if (error) {
						console.error(error);
						reject(error);
					} else { resolve(response); }
				});
		});
	}

	async stat(path) {
		try {
			const metadata = await new Promise((resolve, reject) => {
				this.api().statObject(this.bucket_, this.makePath_(path), (error, response) => {
					if (error) reject(error);
					else resolve(response);
				});
			});
			return this.metadataToStat_(metadata, path);
		} catch (error) {
			if (this.hasErrorCode_(error, 'NotFound')) {
				// ignore
			} else {
				throw error;
			}
		}
	}

	metadataToStat_(md, path) {
		const relativePath = basename(path);
		let isDeleted = false;
		let lastModifiedDate;
		if (md) {
			lastModifiedDate = md['lastModified'] ? new Date(md['lastModified']) : new Date();
		} else {
			lastModifiedDate = new Date();
			isDeleted = true;
		}
		return {
			path: relativePath,
			updated_time: lastModifiedDate.getTime(),
			isDeleted: isDeleted,
			isDir: false,
		};
	}

	metadataToStats_(mds) {
		const output = [];
		for (let i = 0; i < mds.length; i++) {
			output.push(this.metadataToStat_(mds[i], mds[i]));
		}
		return output;
	}

	async setTimestamp() {
		throw new Error('Not implemented'); // Not needed anymore
	}

	async delta(path, options) {
		const getDirStats = async path => {
			const result = await this.list(path);
			return result.items;
		};

		return await basicDelta(path, getDirStats, options);
	}

	async list(path) {
		let prefixPath = this.makePath_(path);
		const pathLen = prefixPath.length;
		if (pathLen > 0 && prefixPath[pathLen - 1] !== '/') {
			prefixPath = `${prefixPath}/`;
		}
		let objs = await this.listObjects(prefixPath);
		if (objs === undefined) objs = [];

		const output = this.metadataToStats_(objs, prefixPath);

		return {
			items: output,
			hasMore: false,
			context: { },
		};
	}

	async get(path, options) {
		const remotePath = this.makePath_(path);
		if (!options) options = {};
		const responseFormat = options.responseFormat || 'text';

		try {
			let output = null;
			let response = null;

			const s3Url = await this.presignedUrl(remotePath);

			if (options.target === 'file') {
				output = await shim.fetchBlob(s3Url, options);
			} else if (responseFormat === 'text') {
				response = await shim.fetch(s3Url, options);

				output = await response.text();
				// we need to make sure that errors get thrown as we are manually fetching above.
				if (!response.ok) {
					throw { name: response.statusText, output: output };
				}
			}

			return output;
		} catch (error) {

			// This means that the error was on the Desktop client side and we need to handle that.
			// On Mobile it won't match because FetchError is a node-fetch feature.
			// https://github.com/node-fetch/node-fetch/blob/main/docs/ERROR-HANDLING.md
			if (error.name === 'FetchError') { throw error.message; }

			let parsedOutput = '';

			// If error.output is not xml the last else case should
			// actually let us see the output of error.
			if (error.output) {
				parsedOutput = parser.parse(error.output);
				if (this.hasErrorCode_(parsedOutput.Error, 'AuthorizationHeaderMalformed')) {
					throw error.output;
				}

				if (this.hasErrorCode_(parsedOutput.Error, 'NoSuchKey')) {
					return null;
				} else if (this.hasErrorCode_(parsedOutput.Error, 'AccessDenied')) {
					throw new JoplinError('Do not have proper permissions to Bucket', 'rejectedByTarget');
				}
			} else {
				if (error.output) {
					throw error.output;
				} else {
					throw error;
				}
			}
		}
	}

	// Don't need to make directories, S3 is key based storage.
	async mkdir() {
		return true;
	}

	async put(path, content, options = null) {
		const remotePath = this.makePath_(path);
		if (!options) options = {};

		// See https://github.com/facebook/react-native/issues/14445#issuecomment-352965210
		if (typeof content === 'string') content = shim.Buffer.from(content, 'utf8');

		try {
			if (options.source === 'file') {
				await this.uploadFileFrom(options.path, remotePath);
				return;
			}

			await this.putObject(remotePath, content);
		} catch (error) {
			if (this.hasErrorCode_(error, 'AccessDenied')) {
				throw new JoplinError('Do not have proper permissions to Bucket', 'rejectedByTarget');
			} else {
				throw error;
			}
		}
	}

	async delete(path) {
		try {
			await this.removeObject(this.makePath_(path));
		} catch (error) {
			if (this.hasErrorCode_(error, 'NoSuchKey')) {
				// ignore
			} else {
				throw error;
			}
		}
	}

	async batchDeletes(paths) {
		const keys = paths.map(path => { return { Key: path }; });
		while (keys.length > 0) {
			const toDelete = keys.splice(0, S3_MAX_DELETES);

			try {
				await this.removeObjects(toDelete);
			} catch (error) {
				if (this.hasErrorCode_(error, 'NoSuchKey')) {
					// ignore
				} else {
					throw error;
				}
			}
		}
	}


	async move(oldPath, newPath) {
		const req = new Promise((resolve, reject) => {
			this.api().copyObject(this.bucket_, this.makePath_(oldPath), newPath,
				(error, response) => {
					if (error) reject(error);
					else resolve(response);
				});
		});

		try {
			await req;

			this.delete(oldPath);
		} catch (error) {
			if (this.hasErrorCode_(error, 'NoSuchKey')) {
				// ignore
			} else {
				throw error;
			}
		}
	}


	format() {
		throw new Error('Not supported');
	}

	async clearRoot() {
		const listRecursive = async () => {
			return new Promise((resolve, reject) => {
				return this.api().listObjectsV2(this.bucket_, '', true, '',
					(error, response) => {
						if (error) reject(error);
						else resolve(response);
					});
			});
		};

		const response = await listRecursive();
		if (response.Contents === undefined) response.Contents = [];
		const keys = response.Contents.map((content) => content.name);

		this.removeObjects(keys);
	}
}

module.exports = { FileApiDriverMinio };
