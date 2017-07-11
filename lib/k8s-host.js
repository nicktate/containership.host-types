'use strict';

const _ = require('lodash');
const HostImplementation = require('@containership/containership.abstraction.host');
const KubernetesApi = require('@containership/containership.k8s.api-bridge');

class K8SHost extends HostImplementation {
    constructor(mode, leaderIP, apiPort, organizationId, clusterId, host_id) {
        const orchestrator = 'kubernetes';

        super(orchestrator, mode, leaderIP, apiPort, host_id);
        this.organizationId = organizationId;
        this.clusterId = clusterId;
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
        return this.clusterId;
    }

}

module.exports = K8SHost;
