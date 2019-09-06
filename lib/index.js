'use strict';
// Load modules

const Stream = require('stream');
const Os = require('os');

 const Stringify = require('fast-safe-stringify');
var net = require('net');

// Declare internals

const internals = {
    defaults: {
        threshold: 2,
        errorThreshold: 0,
        schema: 'good-tcp'
    },
    host: Os.hostname()
};

class GoodTcp extends Stream.Writable {
    constructor(host, port, config) {
        
        config = config || {};
        const settings = Object.assign({}, internals.defaults, config);

        if (settings.errorThreshold === null) {
            settings.errorThreshold = -Infinity;
        }

        super({ objectMode: true, decodeStrings: false });
        this._settings = settings;
        this._data = [];
        this._failureCount = 0;
        this._host = host;
        this._port = port;
        // Standard users
        this.once('finish', () => {
            this._sendMessages();
        });

        this._client = new net.Socket();

        this._client.connect(port, host, function(err) {
            if(err){
                // console.log("Error while connecting to remote:" + err);
                throw Error("Error while connecting to %s:%s", host, port);
            }
        });

        this._client.on('data', function(data) {
            console.log('Received from Server: ' + data);
            // client.destroy(); // kill client after server's response
        });
        
        this._client.on('close', function() {
            console.log('Connection is closed');
        });
        
    }
    _write(data, encoding, callback) {

        this._data.push(data);
        if (this._data.length >= this._settings.threshold) {
            this._sendMessages((err) => {

                if (err && this._failureCount < this._settings.errorThreshold) {
                    this._failureCount++;
                    return callback();
                }

                this._data = [];
                this._failureCount = 0;

                return callback(this._settings.errorThreshold !== -Infinity && err);
            });
        }
        else {
            setImmediate(callback);
        }
    }
    _sendMessages(callback) {
        if(this._client.readyState != 'open'){
            this._client.destroy();
            this._client.connect(this._port, this._host ,(err) => {
                if(err){
                    callback(err);
                }
            });
        }

        const envelope = {
            host: internals.host,
            // schema: this._settings.schema,
            timeStamp: Date.now(),
            events: this._data
        };

        const payload= Stringify(envelope);
//        console.log("Writing to Fluentd:" + payload + "\nResponse:" + client.write(payload, callback));
        this._client.write(payload, callback);
    }
}


module.exports = GoodTcp;
