if (typeof exports != "undefined") {
    CRDT = require('./../crdt.js');
    CRDT = CRDT.CRDT;
}
var state_counter = {
    type: "STATE_Counter",
    propagation: CRDT.STATE_BASED,
    crdt: {
        base_value: {
            state: {dec: [], inc: []}
        },
        getValue: function () {
            var value = 0;
            var decKeys = Object.keys(this.state.dec);
            var incKeys = Object.keys(this.state.inc);
            for (var decKey = 0; decKey < decKeys.length; decKey++) {
                value -= this.state.dec[decKeys[decKey]];
            }
            for (var incKey = 0; incKey < incKeys.length; incKey++) {
                value += this.state.inc[incKeys[incKey]];
            }
            return value;
        },
        operations: {
            increment: function (id, amount) {
                if (!this.state.inc[id])
                    this.state.inc[id] = 0;
                this.state.inc[id] += amount;
                this.stateChanged(amount);
            },
            decrement: function (id, amount) {
                if (!this.state.dec[id])
                    this.state.dec[id] = 0;
                this.state.dec[id] += amount;
                this.stateChanged(-amount);
            }
        },
        merge: function (local, remote) {
            var decKeys = Object.keys(remote.dec);
            var incKeys = Object.keys(remote.inc);

            for (var i = 0; i < decKeys.length; i++) {
                var countLocal = local.dec[decKeys[i]];
                if (!countLocal) {
                    local.dec[decKeys[i]] = remote.dec[decKeys[i]];
                } else {
                    local.dec[decKeys[i]] = Math.max(local.dec[decKeys[i]], remote.dec[decKeys[i]]);
                }
            }

            for (var i = 0; i < incKeys.length; i++) {
                var countLocal = local.inc[incKeys[i]];
                if (!countLocal) {
                    local.inc[incKeys[i]] = remote.inc[incKeys[i]];
                } else {
                    local.inc[incKeys[i]] = Math.max(local.inc[incKeys[i]], remote.inc[incKeys[i]]);
                }
            }

            return local;
        },

        /**
         *
         * @param local
         * @param remote
         * @returns {number}
         */
        compare: function (local, remote) {
            var first = false;
            var second = false;

            var v1Inc = local.state.inc;
            var v2Inc = remote.state.inc;

            var v1Dec = local.state.dec;
            var v2Dec = remote.state.dec;

            if (v1Inc.size() > v2Inc.size() || v1Dec.size() > v2Dec.size()) {
                first = true;
            }

            if (v2Inc.size() > v1Inc.size() || v2Dec.size() > v1Dec.size()) {
                second = true;
            }
            if (first && second) {
                return CRDT.STATE.COMPARE_RESPONSE.MUST_MERGE;
            }

            if (!first) {
                var keysInc1 = v1Inc.keys();
                for (var i = 0; i < keysInc1; i++) {
                    var currKey = keysInc1[i];
                    if (!v2Inc.contains(currKey)) {
                        return CRDT.STATE.COMPARE_RESPONSE.MUST_MERGE;
                    } else {
                        if (v1Inc.get(currKey) > v2Inc.get(currKey)) {
                            first = true;
                            break;
                        }
                    }
                }

                var keysDec1 = v1Dec.keys();
                for (var i = 0; i < keysDec1; i++) {
                    var currKey = keysDec1[i];
                    if (!v2Dec.contains(currKey)) {
                        return CRDT.STATE.COMPARE_RESPONSE.MUST_MERGE;
                    } else {
                        if (v1Dec.get(currKey) > v2Dec.get(currKey)) {
                            first = true;
                            break;
                        }
                    }
                }
            }
            if (!second) {
                var keysInc2 = v2Inc.keys();
                for (var i = 0; i < keysInc2; i++) {
                    var currKey = keysInc2[i];
                    if (!v1Inc.contains(currKey)) {
                        return CRDT.STATE.COMPARE_RESPONSE.MUST_MERGE;
                    } else {
                        if (v2Inc.get(currKey) > v1Inc.get(currKey)) {
                            second = true;
                            break;
                        }
                    }
                }

                var keysDec2 = v2Dec.keys();
                for (var i = 0; i < keysDec2; i++) {
                    var currKey = keysDec2[i];
                    if (!v1Dec.contains(currKey)) {
                        return CRDT.STATE.COMPARE_RESPONSE.MUST_MERGE;
                    } else {
                        if (v2Dec.get(currKey) > v1Dec.get(currKey)) {
                            second = true;
                            break;
                        }
                    }
                }
            }


            if (first && !second) {
                return CRDT.STATE.COMPARE_RESPONSE.LOWER;
            }
            if (!first && second) {
                return CRDT.STATE.COMPARE_RESPONSE.HIGHER;
            }
            if (!first && !second) {
                return CRDT.STATE.COMPARE_RESPONSE.EQUALS;
            }
            return CRDT.STATE.COMPARE_RESPONSE.MUST_MERGE;
        },
        fromJSONString: function (jsObject) {
            var ret = {dec: [], inc: []};

            var dec = jsObject.dec;
            var inc = jsObject.inc;

            for (var i = 0; i < dec.length; i++) {
                ret.dec[dec[i][0]] = dec[i][1];
            }
            for (var i = 0; i < inc.length; i++) {
                ret.inc[inc[i][0]] = inc[i][1];
            }
            return ret;
        },
        toJSONString: function (state) {
            var decKeys = Object.keys(state.dec);
            var incKeys = Object.keys(state.inc);

            var dec = [];
            var inc = [];

            for (var i = 0; i < decKeys.length; i++) {
                dec.push([decKeys[i], state.dec[decKeys[i]]]);
            }
            for (var i = 0; i < incKeys.length; i++) {
                inc.push([incKeys[i], state.inc[incKeys[i]]]);
            }

            return {dec: dec, inc: inc};
        }
    }
};
if (typeof exports != "undefined") {
    exports.STATE_Counter = state_counter;
} else {
    CRDT_LIB.STATE_Counter = state_counter;
}