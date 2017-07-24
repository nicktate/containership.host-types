'use strict';

const _ = require('lodash');
const async = require('async');
const constants = require('containership.core.constants');
const HostImplementation = require('@containership/containership.abstraction.host');
const KubernetesApi = require('@containership/containership.k8s.api-bridge');

const CONSTRAINT_ENFORCEMENT_INTERVAL = (60 * 1000);

class K8SHost extends HostImplementation {
    constructor(host_id, cluster_id, mode, api_ip, api_port, k8s_api_ip, k8s_api_port, preferred_nic) {
        // TODO - Update to support opensource installs
        if(!cluster_id) {
            throw new Error('You must pass cluster_id when instantiating a K8S Host!');
        }

        const orchestrator = 'kubernetes';
        super(orchestrator, host_id, mode, api_ip, api_port, preferred_nic);

        this.api_ip = this.api_ip || 'localhost';
        this.api_port = this.api_port || 8080;
        this.is_ready = false;
        this.ready_queue = [];

        this.attrs = {};

        this.api = new KubernetesApi(k8s_api_ip, k8s_api_port);

        // leaders that join cluster should update cluster id
        // TODO: update for multi-master support
        if(this.getOperatingMode() === 'leader') {
            setInterval(() => {
                this.api.enforceAllConstraints();
            }, CONSTRAINT_ENFORCEMENT_INTERVAL);

            setInterval(() => {
                this.api.enforceNodeLiveliness();
            }, CONSTRAINT_ENFORCEMENT_INTERVAL);

            this._setDistributedClusterId(cluster_id);
        }

        // cluster_id explicitly passed in
        this.setClusterId(cluster_id);
        this.is_ready = true;
        this._triggerReadyQueue();
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

    _setDistributedClusterId(cluster_id) {
        return async.retry({
            times: 8,
            interval: function(attempt) {
                // retry intervals (500, 1000, 2000, 4000, 8000, 16000, 32000, 64000, ... milliseconds)
                return 250 * Math.pow(2, attempt);
            }
        }, (cb) => {
            return this.api.setDistributedKey(constants.myriad.CLUSTER_ID, cluster_id, (err) => {
                if(err) {
                    console.warn('Failed to set distributed key: ', err);
                    return cb(err);
                }

                return cb();
            });
        }, (err) => {
            if(err) {
                return console.error('Async retry failed to set the cluster_id: ', err);
            }

            return console.info(`Successfully set the cluster_id distributed key: ${cluster_id}`);
        });
    }

    once(event, callback) {
        if(event === 'ready') {
            if(this.is_ready) {
                return callback();
            }

            this.ready_queue.push(callback);
        } else {
            console.warn(`Only supported event type is "ready", ignoring "${event}" invocation of K8SHost.once`);
        }
    }

    _triggerReadyQueue() {
        _.forEach(this.ready_queue, (cb) => cb());
        this.ready_queue = [];
    }
}

module.exports = K8SHost;
