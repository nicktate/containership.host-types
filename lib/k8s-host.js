'use strict';

const _ = require('lodash');
const constants = require('containership.core.constants');
const HostImplementation = require('@containership/containership.abstraction.host');
const KubernetesApi = require('@containership/containership.k8s.api-bridge');
const uuid = require('node-uuid');

const CONSTRAINT_ENFORCEMENT_INTERVAL = (60 * 1000);

class K8SHost extends HostImplementation {
    constructor(host_id, cluster_id, mode, api_ip, api_port) {
        const orchestrator = 'kubernetes';

        super(orchestrator, host_id, mode, api_ip, api_port);

        this.api_ip = this.api_ip || 'localhost';
        this.api_port = this.api_port || 8080;

        this.attrs = {};

        // TODO remove hardcoded port
        this.api = new KubernetesApi(this.getApiIp(), this.getApiPort());

        if(this.getOperatingMode() === 'leader') {
            setInterval(() => {
                this.api.enforceAllConstraints();
            }, CONSTRAINT_ENFORCEMENT_INTERVAL);
        }

        // get cluster id from distributed state
        this.api.getDistributedKey(constants.myriad.CLUSTER_ID, (err, cluster_id) => {
            if(!err && cluster_id) {
                this.setClusterId(cluster_id);
            }

            let error_connecting = false;

            // listen for changes to cluster id from the leader and set interally accordingly
            const cluster_id_listener = this.api.subscribeDistributedKey(constants.myriad.CLUSTER_ID);
            cluster_id_listener.on('message', (cluster_id) => {
                if(error_connecting) {
                    error_connecting = false;
                }

                this.setClusterId(cluster_id);
            });

            cluster_id_listener.on('error', (err) => {
                if(!error_connecting) {
                    console.error(`Error setting cluster_id: ${err}`)
                    error_connecting = true;
                }
            });

            // leaders that join cluster should update cluster id
            // TODO: update for multi-master support
            if(this.getOperatingMode() === 'leader') {
                // set a default cluster id if one does not exist
                if(!cluster_id) {
                    cluster_id = uuid.v4();
                }

                this.api.setDistributedKey(constants.myriad.CLUSTER_ID, cluster_id, () => {});
            }
        });
    }

    getApi() {
        return this.api;
    }

    getAttributes() {
        return this.attrs;
    }

    setAttributes(attrs) {
        this.attrs = _.merge(this.attrs, attrs);
    }
}

module.exports = K8SHost;
