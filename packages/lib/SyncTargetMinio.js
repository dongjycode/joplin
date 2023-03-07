const BaseSyncTarget = require('./BaseSyncTarget').default;
const { _ } = require('./locale');
const Setting = require('./models/Setting').default;
const { FileApi } = require('./file-api.js');
const Synchronizer = require('./Synchronizer').default;
const { FileApiDriverMinio } = require('./file-api-driver-minio.js');
const { Client } = require('minio');


class SyncTargetMinio extends BaseSyncTarget {
	static id() {
		return 11;
	}

	static supportsConfigCheck() {
		return true;
	}

	constructor(db, options = null) {
		super(db, options);
		this.api_ = null;
	}

	static targetName() {
		return 'minio';
	}

	static label() {
		return `${_('MinIO')}`;
	}

	static description() {
		return 'A service offered by Minio that provides object storage through a web service interface.';
	}

	async isAuthenticated() {
		return true;
	}

	static minioBucketName() {
		return Setting.value('sync.11.path');
	}

	// These are the settings that get read from disk to instantiate the API.
	minioAuthParameters() {
		return {
			endPoint: Setting.value('sync.11.endpoint'),
			port: Setting.value('sync.11.port'),
			accessKey: Setting.value('sync.11.accessKey'),
			secretKey: Setting.value('sync.11.secretKey'),
			useSSL: Setting.value('sync.11.useSSL'),
		};
	}

	api() {
		if (this.api_) return this.api_;

		this.api_ = new Client(this.minioAuthParameters());
		return this.api_;
	}

	static async newFileApi_(syncTargetId, options) {
		// These options are read from the form on the page
		// so we can test new config choices without overriding the current settings.
		const apiOptions = {
			endPoint: options.endpoint(),
			port: options.port(),
			// region: options.region(),
			secretKey: options.secretKey(),
			accessKey: options.accessKey(),
			useSSL: options.useSSL(),
		};
		const api = new Client(apiOptions);
		const driver = new FileApiDriverMinio(api, SyncTargetMinio.minioBucketName());
		const fileApi = new FileApi('', driver);
		fileApi.setSyncTargetId(syncTargetId);
		return fileApi;
	}

	// With the aws-sdk-v3-js some errors (301/403) won't get their XML parsed properly.
	// I think it's this issue: https://github.com/aws/aws-sdk-js-v3/issues/1596
	// If you save the config on desktop, restart the app and attempt a sync, we should get a clearer error message because the sync logic has more robust XML error parsing.
	// We could implement that here, but the above workaround saves some code.

	static async checkConfig(options) {
		const output = {
			ok: false,
			errorMessage: '',
		};
		try {
			const fileApi = await SyncTargetMinio.newFileApi_(SyncTargetMinio.id(), options);
			fileApi.requestRepeatCount_ = 0;

			const headBucketReq = new Promise((resolve, reject) => {
				fileApi.driver().api().bucketExists(
					options.path(), (error, response) => {
						if (error) reject(error);
						else resolve(response);
					});
			});
			const result = await headBucketReq;

			if (!result) throw new Error(`Minio bucket not found: ${SyncTargetMinio.minioBucketName()}`);
			output.ok = true;
		} catch (error) {
			if (error.message) {
				output.errorMessage = error.message;
			}
			if (error.code) {
				output.errorMessage += ` (Code ${error.code})`;
			}
		}

		return output;
	}

	async initFileApi() {
		const appDir = '';
		const fileApi = new FileApi(appDir, new FileApiDriverMinio(this.api(), SyncTargetMinio.minioBucketName()));
		fileApi.setSyncTargetId(SyncTargetMinio.id());

		return fileApi;
	}

	async initSynchronizer() {
		return new Synchronizer(this.db(), await this.fileApi(), Setting.value('appType'));
	}
}

module.exports = SyncTargetMinio;
