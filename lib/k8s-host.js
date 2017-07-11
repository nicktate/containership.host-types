'use strict';

const _ = require('lodash');
const HostImplementation = require('@containership/containership.abstraction.host');
const KubernetesApi = require('@containership/containership.k8s.api-bridge');

class K8SHost extends HostImplementation {
    constructor(host_id, cluster_id, mode, leader_ip, api_port) {
        const orchestrator = 'kubernetes';

        super(orchestrator, mode, leader_ip, api_port, host_id);

        this.cluster_id = cluster_id;
        this.attrs = {};

        this.api = new KubernetesApi(this.getApiIp(), 8080);
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

    getClusterId() {
        return this.cluster_id;
    }

}

module.exports = K8SHost;
