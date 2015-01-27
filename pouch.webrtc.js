/*jslint nomen: true, browser:true, todo:true*/
/*globals PouchDB, Blob, FileReader*/

"use strict";

var PeerPouch,
    RPCHandler,
    RTCPeerConnection = window.mozRTCPeerConnection || window.RTCPeerConnection || window.webkitRTCPeerConnection,
    RTCSessionDescription = window.mozRTCSessionDescription || window.RTCSessionDescription || window.webkitRTCSessionDescription,
    RTCIceCandidate = window.mozRTCIceCandidate || window.RTCIceCandidate || window.webkitRTCIceCandidate,
    PeerConnectionHandler;

// a couple additional errors we use
PouchDB.Errors.NOT_IMPLEMENTED = {
    status: 501,
    error: 'not_implemented',
    reason: "Unable to fulfill the request"
};       // [really for METHODs only?]

PouchDB.Errors.FORBIDDEN = {
    status: 403,
    error: 'forbidden',
    reason: "The request was refused"
};

RPCHandler = function (tube) {
    var blobsForNextCall,
        _isBlob;

    _isBlob = function (obj) {
        var type = Object.prototype.toString.call(obj);
        return (type === '[object Blob]' || type === '[object File]');
    };

    this.onbootstrap = null;        // caller MAY provide this

    this._exposed_fns = Object.create(null);
    this.serialize = function (obj) {
        var messages = [];
        /*jslint unparam:true*/
        messages.push(JSON.stringify(obj, function (k, v) {
            var id,
                n;
            if (typeof v === 'function') {
                id = Math.random().toFixed(20).slice(2);
                this._exposed_fns[id] = v;
                return {
                    __remote_fn: id
                };
            }
            if (Object.prototype.toString.call(v) === '[object IDBTransaction]') {
                // HACK: pouch.idb.js likes to bounce a ctx object around but if we null it out it recreates
                // c.f. https://github.com/daleharvey/pouchdb/commit/e7f66a02509bd2a9bd12369c87e6238fadc13232
                return;

                // TODO: the WebSQL adapter also does this but does NOT create a new transaction if it's missing :-(
                // https://github.com/daleharvey/pouchdb/blob/80514c7d655453213f9ca7113f327424969536c4/src/adapters/pouch.websql.js#L646
                // so we'll have to either get that fixed upstream or add remote object references (but how to garbage collect? what if local uses?!)
            }
            if (_isBlob(v)) {
                n = messages.indexOf(v) + 1;
                if (!n) {
                    n = messages.push(v);
                }
                return {
                    __blob: n
                };
            }
            return v;
        }.bind(this)));
        /*jslint unparam: false*/
        return messages;
    };

    blobsForNextCall = [];                  // each binary object is sent before the function call message
    this.deserialize = function (data) {
        if (typeof data === 'string') {
            /*jslint unparam:true*/
            return JSON.parse(data, function (k, v) {
                var b;
                if (v && v.__remote_fn) {
                    return function () {
                        this._callRemote(v.__remote_fn, arguments);
                    }.bind(this);
                }
                if (v && v.__blob) {
                    b = blobsForNextCall[v.__blob - 1];
                    if (!_isBlob(b)) {
                        b = new Blob([b]);       // `b` may actually be an ArrayBuffer
                    }
                    return b;
                }
                return v;
            }.bind(this));
            /*jslint unparam:false*/
            // blobsForNextCall.length = 0;  Unreachable after all these returns;
        }
        blobsForNextCall.push(data);
    };

    this._callRemote = function (fn, args) {
        var processNext,
            messages;

        messages = this.serialize({
            fn: fn,
            args: Array.prototype.slice.call(args)
        });

        // WORKAROUND: Chrome (as of M32) cannot send a Blob, only an ArrayBuffer. So we send each once converted…
        processNext = function () {
            var msg = messages.shift(),
                reader;
            if (!msg) {
                return;
            }
            if (_isBlob(msg)) {
                reader = new FileReader();
                reader.readAsArrayBuffer(msg);
                reader.onload = function () {
                    tube.send(reader.result);
                    processNext();
                };
            } else {
                tube.send(msg);
                processNext();
            }
        };
        if (window.mozRTCPeerConnection) {
            messages.forEach(function (msg) { tube.send(msg); });
        } else {
            processNext();
        }
    };

    this._exposed_fns['__BOOTSTRAP__'] = function () {
        if (this.onbootstrap) {
            this.onbootstrap.apply(this, arguments);
        }
    }.bind(this);

    tube.onmessage = function (evt) {
        var call = this.deserialize(evt.data),
            fn;
        if (!call) {
            return;      //
        }

        fn = this._exposed_fns[call.fn];
        if (!fn) {
            console.warn("RPC call to unknown local function", call);
            return;
        }

        // leak only callbacks which are marked for keeping (most are one-shot)
        if (!fn._keep_exposed) {
            delete this._exposed_fns[call.fn];
        }

        try {
//console.log("Calling RPC", fn, call.args);
            fn.apply(null, call.args);
        } catch (e) {           // we do not signal exceptions remotely
            console.warn("Local RPC invocation unexpectedly threw:", e);
        }
    }.bind(this);
};

RPCHandler.prototype.bootstrap = function () {
    this._callRemote('__BOOTSTRAP__', arguments);
};

// Implements the API for dealing with a PouchDB peer's database over WebRTC
PeerPouch = function (opts, callback) {
    var _init,
        handler,
        api,
        TODO;

    TODO = function (callback) {
        // TODO: callers of this function want implementations
        if (callback) {
            setTimeout(function () { callback(PouchDB.Errors.NOT_IMPLEMENTED); }, 0);
        }
    };

    _init = PeerPouch._shareInitializersByName[opts.name];
    if (!_init) {
        throw new Error("Unknown PeerPouch share dbname");      // TODO: use callback instead?
    }

    handler = _init(opts);
    api = {};       // initialized later, but Pouch makes us return this before it's ready

    handler.onconnection = function () {
        var rpc = new RPCHandler(handler._tube());
        rpc.onbootstrap = function (d) {      // share will bootstrap
            var rpcAPI = d.api;

            // simply hook up each [proxied] remote method as our own local implementation
            Object.keys(rpcAPI).forEach(function (key) {
                api[key] = rpcAPI[key];
            });

            // one override to provide a synchronous `.cancel()` helper locally
            api._changes = function (opts) {
                var cancelRemotely,
                    cancelledLocally;
                if (opts.onChange) {
                    opts.onChange._keep_exposed = true;      // otherwise the RPC mechanism tosses after one use
                }
                cancelRemotely = null;
                cancelledLocally = false;
                rpcAPI._changes(opts, function (rpcCancel) {
                    if (cancelledLocally) {
                        rpcCancel();
                    } else {
                        cancelRemotely = rpcCancel;
                    }
                });
                return {
                    cancel: function () {
                        if (cancelRemotely) {
                            cancelRemotely();
                        } else {
                            cancelledLocally = true;
                        }
                        if (opts.onChange) {
                            delete opts.onChange._keep_exposed;  // allow for slight chance of cleanup [if called again]
                        }
                    }
                };
            };

//            api._id = function () {
//                // TODO: does this need to be "mangled" to distinguish it from the real copy?
//                //       [it seems unnecessary: a replication with "api" is a replication with "rpcAPI"]
//                return rpcAPI._id;
//            };
//             api._id = pouchdb.utils.topromise(function (callback) {
//                 callback(null, rpcapi._id);
//             });
            api._id = function () {
                return PouchDB.utils.Promise.resolve(rpcAPI._id);
            };
            // now our api object is *actually* ready for use
            if (callback) {
                callback(null, api);
            }
        };
    };

    return api;
};

PeerPouch._wrappedAPI = function (db) {
    /*
        This object will be sent over to the remote peer. So, all methods on it must be:
        - async-only (all "communication" must be via callback, not exceptions or return values)
        - secure (peer could provide untoward arguments)
    */
    var rpcAPI = {},
    /*
        This lists the core "customApi" methods that are expected by pouch.adapter.js
    */
        methods = [
            'bulkDocs',
            '_getRevisionTree',
            '_doCompaction',
            '_get',
            '_getAttachment',
            '_allDocs',
            '_changes',
            '_close',
            '_info',
            '_id'
        ];

    // most methods can just be proxied directly
    methods.forEach(function (method) {
        rpcAPI[method] = db[method];
        if (rpcAPI[method]) {
            rpcAPI[method]._keep_exposed = true;
        }
    });

    // one override, to pass the `.cancel()` helper via callback to the synchronous override on the other side
    rpcAPI._changes = function (opts, rpcCB) {
        var retval = db._changes(opts);
        rpcCB(retval.cancel);
    };

    rpcAPI._changes._keep_exposed = true;

    // just send the local result
    rpcAPI._id = db.id();

    return rpcAPI;
};

// Don't bother letting peers nuke each others' databases
/*jslint unparam:true*/
PeerPouch.destroy = function (name, callback) {
    if (callback) {
        setTimeout(function () { callback(PouchDB.Errors.FORBIDDEN); }, 0);
    }
};
/*jslint unparam:false*/

// Can we breathe in this environment?
PeerPouch.valid = function () {
    // TODO: check for WebRTC+DataConnection support
    return true;
};


PeerPouch._types = {
    presence: 'com.stemstorage.peerpouch.presence',
    signal: 'com.stemstorage.peerpouch.signal',
    share: 'com.stemstorage.peerpouch.share'
};

// Register for our scheme
PouchDB.adapter('webrtc', PeerPouch);


RTCPeerConnection = window.mozRTCPeerConnection || window.RTCPeerConnection || window.webkitRTCPeerConnection,
RTCSessionDescription = window.mozRTCSessionDescription || window.RTCSessionDescription || window.webkitRTCSessionDescription,
RTCIceCandidate = window.mozRTCIceCandidate || window.RTCIceCandidate || window.webkitRTCIceCandidate;

PeerConnectionHandler = function (opts) {
    opts.reliable = true;
    var cfg = {
            "iceServers": [
                {"url": "stun:23.21.150.121"}
            ]
        },
        con = (opts.reliable) ? {} : { 'optional': [{'RtpDataChannels': true }] },
        handler,
        rtc;

    this._rtc = new RTCPeerConnection(cfg, con);

    this.LOG_SELF = opts._self;
    this.LOG_PEER = opts._peer;
    this._channel = null;

    this.onhavesignal = null;       // caller MUST provide this
    this.onreceivemessage = null;   // caller SHOULD provide this
    this.onconnection = null;       // …and maybe this

    handler = this;
    rtc = this._rtc;
    if (opts.initiate) {
        this._setupChannel();
    } else {
        rtc.ondatachannel = this._setupChannel.bind(this);
    }
    rtc.onnegotiationneeded = function () {
        if (handler.DEBUG) {
            console.log(handler.LOG_SELF, "saw negotiation trigger and will create an offer");
        }
        rtc.createOffer(function (offerDesc) {
            if (handler.DEBUG) {
                console.log(handler.LOG_SELF, "created offer, sending to", handler.LOG_PEER);
            }
            rtc.setLocalDescription(offerDesc);
            handler._sendSignal(offerDesc);
        }, function (e) {
            console.warn(handler.LOG_SELF, "failed to create offer", e);
        });
    };
    rtc.onicecandidate = function (evt) {
        if (evt.candidate) {
            handler._sendSignal({candidate: evt.candidate});
        }
    };
    // debugging
    rtc.onicechange = function () {
        if (handler.DEBUG) {
            console.log(handler.LOG_SELF, "ICE change", rtc.iceGatheringState, rtc.iceConnectionState);
        }
    };
    rtc.onstatechange = function () {
        if (handler.DEBUG) {
            console.log(handler.LOG_SELF, "State change", rtc.signalingState, rtc.readyState);
        }
    };
}

PeerConnectionHandler.prototype._sendSignal = function (data) {
    if (!this.onhavesignal) {
        throw new Error("Need to send message but `onhavesignal` handler is not set.");
    }
    this.onhavesignal({
        target: this,
        signal: JSON.parse(JSON.stringify(data))
    });
};

PeerConnectionHandler.prototype.receiveSignal = function (data) {
    var handler = this,
        rtc = this._rtc;
    if (handler.DEBUG) {
        console.log(this.LOG_SELF, "got data", data, "from", this.LOG_PEER);
    }
    if (data.sdp) {
        rtc.setRemoteDescription(new RTCSessionDescription(data), function () {
            var needsAnswer = (rtc.remoteDescription.type === 'offer');
            if (handler.DEBUG) {
                console.log(handler.LOG_SELF, "set offer, now creating answer:", needsAnswer);
            }
            if (needsAnswer) {
                rtc.createAnswer(function (answerDesc) {
                    if (handler.DEBUG) {
                        console.log(handler.LOG_SELF, "got anwer, sending back to", handler.LOG_PEER);
                    }
                    rtc.setLocalDescription(answerDesc);
                    handler._sendSignal(answerDesc);
                }, function (e) {
                    console.warn(handler.LOG_SELF, "couldn't create answer", e);
                });
            }
        }, function (e) {
            console.warn(handler.LOG_SELF, "couldn't set remote description", e);
        });
    } else if (data.candidate) {
        try {
            rtc.addIceCandidate(new RTCIceCandidate(data.candidate));
        } catch (e) {
            console.error("Couldn't add candidate", e);
        }
    }
};

PeerConnectionHandler.prototype.sendMessage = function (data) {
    if (!this._channel || this._channel.readyState !== 'open') {
        throw new Error("Connection exists, but data channel is not open.");
    }
    this._channel.send(data);
};

PeerConnectionHandler.prototype._setupChannel = function (evt) {
    var handler = this, rtc = this._rtc;
    if (evt) {
        if (handler.DEBUG) {
            console.log(this.LOG_SELF, "received data channel", evt.channel.readyState);
        }
    }
    this._channel = evt ? evt.channel : rtc.createDataChannel('peerpouch-dev');
    // NOTE: in Chrome (M32) `this._channel.binaryType === 'arraybuffer'` instead of blob
    this._channel.onopen = function () {
        if (handler.DEBUG) {
            console.log(handler.LOG_SELF, "DATA CHANNEL IS OPEN", handler._channel);
        }
        if (handler.onconnection) {
            handler.onconnection(handler._channel);        // BOOM!
        }
    };
    this._channel.onmessage = function (evt) {
        if (handler.DEBUG) {
            console.log(handler.LOG_SELF, "received message!", evt);
        }
        if (handler.onreceivemessage) {
            handler.onreceivemessage({target: handler, data: evt.data});
        }
    };
    if (window.mozRTCPeerConnection) {
        setTimeout(function () {
            rtc.onnegotiationneeded();     // FF doesn't trigger this for us like Chrome does
        }, 0);
    }
    window.dbgChannel = this._channel;
};

PeerConnectionHandler.prototype._tube = function () {      // TODO: refactor PeerConnectionHandler to simply be the "tube" itself
    var tube = {},
        handler = this;
    tube.onmessage = null;
    tube.send = function (data) {
        handler.sendMessage(data);
    };
    handler.onreceivemessage = function (evt) {
        if (tube.onmessage) {
            tube.onmessage(evt);
        }
    };
    return tube;
};


var SharePouch = function () {
    // NOTE: this plugin's methods are intended for use only on a **hub** database

    // this chunk of code manages a combined _changes listener on hub for any share/signal(/etc.) watchers
    var watcherCount = 0,         // easier to refcount than re-count!
        watchersByType = {},
        changesListener = null,
        sharesByRemoteId = {},         // ._id of share doc
        sharesByLocalId = {},            // .id() of database
        addWatcher,     // internal functions
        removeWatcher,
        _localizeShare,
        _isLocal,
        share,          // 'exported' functions
        unshare,
        getShares;

    addWatcher = function (type, cb) {
        var watchers,
            cancelListen,
            that = this;

        watchersByType[type] = watchersByType[type] || [];
        watchers = watchersByType[type];
        watchers.push(cb);
        watcherCount += 1;

        if (watcherCount > 0 && !changesListener) {         // start listening for changes (at current sequence)
            cancelListen = false;
            changesListener = {
                cancel: function () {
                    cancelListen = true;
                }
            };
            that.info(function (err, info) {
                var opts;
                if (err) {
                    throw new Error(err);
                }
                opts = {
                    //filter: PeerPouch.PeerPouch._types.ddoc_name+'/signalling',             // see https://github.com/daleharvey/pouchdb/issues/525
                    include_docs: true,
                    continuous: true,
                    since: info.update_seq
                };
                opts.onChange = function (change) {
                    var watchersOnChange = watchersByType[change.doc.type];
                    if (watchersOnChange) {
                        watchersOnChange.forEach(function (cb) {
                            cb(change.doc);
                        });
                    }
                };
                if (!cancelListen) {
                    changesListener = that.changes(opts);
                } else {
                    changesListener = null;
                }
            });
        }
        return {
            cancel: function () {
                removeWatcher(type, cb);
            }
        };
    };

    removeWatcher = function (type, cb) {
        var watchers = watchersByType[type],
            cbIdx = watchers ? watchers.indexOf(cb) : -1;
        if (~cbIdx) {
            watchers.splice(cbIdx, 1);
            watcherCount -= 1;
        }
        if (watcherCount < 1 && changesListener) {
            changesListener.cancel();
            changesListener = null;
        }
    };

    share = function (db, opts, cb) {
        var shareObj,
            peerHandlers,
            that = this;
        if (typeof opts === 'function') {
            cb = opts;
            opts = {};
        } else {
            opts = opts || {};
        }

        shareObj = {
            _id: 'share-' + PouchDB.utils.uuid(),
            type: PeerPouch._types.share,
            name: opts.name || null,
            info: opts.info || null
        };

        that.post(shareObj, function (err, doc) {
            if (!err) {
                shareObj._rev = doc.rev;
            }
            if (cb) {
                cb(err, doc);
            }
        });

        peerHandlers = Object.create(null);
        shareObj._signalWatcher = addWatcher.call(that, PeerPouch._types.signal, function receiveSignal(signal) {
            var self = shareObj._id,
                peer = signal.sender,
                info = signal.info,
                handler = peerHandlers[peer];

            if (signal.recipient !== shareObj._id) {
                return;
            }

            if (!handler) {
                handler = peerHandlers[peer] = new PeerConnectionHandler({initiate: false, _self: self, _peer: peer});
                handler.onhavesignal = function sendSignal(evt) {
                    that.post({
                        _id: 's-signal-' + PouchDB.utils.uuid(),
                        type: PeerPouch._types.signal,
                        sender: self,
                        recipient: peer,
                        data: evt.signal,
                        info: shareObj.info
                    }, function (err) {
                        if (err) {
                            throw new Error(err);
                        }
                    });
                };
                handler.onconnection = function () {
                    var evt,
                        cancelled,
                        rpc;
                    if (opts.onRemote) {
                        evt = {info: info};
                        cancelled = false;
                        evt.preventDefault = function () {
                            cancelled = true;
                        };
                        opts.onRemote.call(handler._rtc, evt);            // TODO: this is [still] likely to change!
                        if (cancelled) {
                            return;       // TODO: close connection
                        }
                    }
                    rpc = new RPCHandler(handler._tube());
                    rpc.bootstrap({
                        api: PeerPouch._wrappedAPI(db)
                    });
                };
            }
            handler.receiveSignal(signal.data);
            that.post({
                _id: signal._id,
                _rev: signal._rev,
                _deleted: true
            }, function (err) {
                if (err) {
                    console.warn("Couldn't clean up signal", err);
                }
            });
        });
        sharesByRemoteId[shareObj._id] = sharesByLocalId[db.id()] = shareObj;
    };

    unshare = function (db, cb) {            // TODO: call this automatically from _delete hook whenever it sees a previously shared db?
        var shareloc = sharesByLocalId[db.id()];
        if (!shareloc) {
            if (typeof cb === 'function') {
                return setTimeout(function () {
                    cb(new Error("Database is not currently shared"));
                }, 0);
            }
            return false;
        }
        this.post({
            _id: shareloc._id,
            _rev: shareloc._rev,
            _deleted: true
        }, cb);
        shareloc._signalWatcher.cancel();
        delete sharesByRemoteId[shareloc._id];
        delete sharesByLocalId[db.id()];
    };

    _localizeShare = function (doc) {
        var name = [this.id(), doc._id].map(encodeURIComponent).join('/'),
            that = this;

        if (doc._deleted) {
            delete PeerPouch._shareInitializersByName[name];
        } else {
            PeerPouch._shareInitializersByName[name] = function (opts) {
                var client = 'peer-' + PouchDB.utils.uuid(),
                    shareloc = doc._id,
                    handler = new PeerConnectionHandler({
                        initiate: true,
                        _self: client,
                        _peer: shareloc
                    });
                handler.onhavesignal = function sendSignal(evt) {
                    that.post({
                        _id: 'p-signal-' + PouchDB.utils.uuid(),
                        type: PeerPouch._types.signal,
                        sender: client,
                        recipient: shareloc,
                        data: evt.signal,
                        info: opts.info
                    }, function (err) {
                        if (err) {
                            throw new Error(err);
                        }
                    });
                };
                addWatcher.call(that, PeerPouch._types.signal, function receiveSignal(signal) {
                    if (signal.recipient !== client || signal.sender !== shareloc) {
                        return;
                    }
                    handler.receiveSignal(signal.data);
                    that.post({
                        _id: signal._id,
                        _rev: signal._rev,
                        _deleted: true
                    }, function (err) {
                        if (err) {
                            console.warn("Couldn't clean up signal", err);
                        }
                    });
                });

                return handler;     /* for .onreceivemessage and .sendMessage use */
            };
        }
        doc.dbname = 'webrtc://' + name;
        return doc;
    };

    _isLocal = function (doc) {
        if (typeof sharesByRemoteId !== 'object') {
            return false;
        }
        return sharesByRemoteId.hasOwnProperty(doc._id);
    };

    getShares = function (opts, cb) {
        var that = this;
        if (typeof opts === 'function') {
            cb = opts;
            opts = {};
        }
        opts = opts || {};
        //hub.query(PeerPouch._types.ddoc_name+'/shares', {include_docs:true}, function (e, d) {
        this.allDocs({include_docs: true}, function (err, docs) {
            if (err) {
                cb(err);
                return;
            }
            cb(null, docs.rows.filter(function (r) {
                return (r.doc.type === PeerPouch._types.share && !_isLocal(r.doc));
            }).map(function (r) {
                return _localizeShare.call(that, r.doc);
            }));
        });
        if (opts.onChange) {            // WARNING/TODO: listener may get changes before cb returns initial list!
            return addWatcher.call(this, PeerPouch._types.share, function (doc) {
                if (!_isLocal(doc)) {
                    opts.onChange(_localizeShare.call(that, doc));
                }
            });
        }
    };

    return {
        shareDatabase: share,
        unshareDatabase: unshare,
        getSharedDatabases: getShares
    };
};

PeerPouch._shareInitializersByName = Object.create(null);           // global connection between new PeerPouch (client) and source SharePouch (hub)

SharePouch._delete = function () {};            // blindly called by PouchDB.destroy

PouchDB.plugin(new SharePouch());

PouchDB.dbgPeerPouch = PeerPouch;
