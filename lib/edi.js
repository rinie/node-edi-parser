// Module EDI - Copyright Rinie Kervel (BSD Licensed)
// Based on Module CSV Copyright David Worms <open@adaltas.com> (BSD Licensed)

var EventEmitter = require('events').EventEmitter,
    fs = require('fs');

// Utils function
var merge = function(obj1,obj2){
    var r = obj1||{};
    for(var key in obj2){
        r[key] = obj2[key];
    }
    return r;
}

module.exports = function(){
    var state = {
        count: 0,
        countWriten: 0,
        field: '',
        line: [],
        lastC: '',
        quoted: false,
        commented: false,
        buffer: null,
        bufferPosition: 0
    }
    // Are we currently inside the transform callback? If so,
    // we shouldn't increment `state.count` which count provided lines
    var transforming = false;
    
    // Defined Class
    
    var EDI = function(){
        // Set options
        this.readOptions = {
            delimiter: ',',
            quote: '"',
            escape: '"',
            columns: null,
            flags: 'r',
            encoding: 'utf8',
            bufferSize: 8 * 1024 * 1024,
            trim: false,
            ltrim: false,
            rtrim: false
        };
        this.writeOptions = {
            delimiter: null,
            quote: null,
            escape: null,
            columns: null,
            header: false,
            lineBreaks: null,
            flags: 'w',
            encoding: 'utf8',
            bufferSize: null,
            newColumns: false,
            end: true // Call `end()` on close
        };
        // A boolean that is true by default, but turns false after an 'error' occurred, 
        // the stream came to an 'end', or destroy() was called. 
        this.readable = true;
        // A boolean that is true by default, but turns false after an 'error' occurred 
        // or end() / destroy() was called. 
        this.writable = true;
    }
    EDI.prototype.__proto__ = EventEmitter.prototype;
    
    // Reading API
    
    EDI.prototype.from = function(data,options){
        if(options) merge(this.readOptions,options);
        var self = this;
        process.nextTick(function(){
            if(data instanceof Array){
                if( edi.writeOptions.lineBreaks === null ){
                    edi.writeOptions.lineBreaks = "\n";
                }
                for(var i=0; i<data.length; i++){
                    state.line = data[i];
                    flush();
                }
            }else{
                try{
                    parse(data);
                }catch(e){
                    self.emit('error', e);
                    return;
                }
            }
            self.end();
        });
        return this;
    }
    EDI.prototype.fromStream = function(readStream, options){

        if(options) merge(this.readOptions,options);
        var self = this;
        readStream.on('data', function(data) { 
            try{
                parse(data);
            }catch(e){
                self.emit('error', e);
                // Destroy the input stream
                readStream.destroy();
            }
        });
        readStream.on('error', function(error) { self.emit('error', error) });
        readStream.on('end', function() {
            self.end();
        });
        this.readStream = readStream;
        return this;
    }
    EDI.prototype.fromPath = function(path, options){
        if(options) merge(this.readOptions,options);
        var stream = fs.createReadStream(path, this.readOptions);
        stream.setEncoding(this.readOptions.encoding);
        return this.fromStream(stream, null);
    }
    
    // Writting API
    
    /**
     * Write data.
     * Data may be string in which case it could span multiple lines. If data 
     * is an object or an array, it must represent a single line.
     * Preserve is for line which are not considered as EDI data.
     */
    EDI.prototype.write = function(data, preserve){
        if(typeof data === 'string' && !preserve){
            return parse(data);
        }else if(Array.isArray(data) && !transforming){
            state.line = data;
            return flush();
        }
        if(state.count === 0 && edi.writeOptions.header === true){
            write(edi.writeOptions.columns || edi.readOptions.columns);
        }
        write(data, preserve);
        if(!transforming && !preserve){
            state.count++;
        }
    }
    
    EDI.prototype.end = function(){
        if (state.quoted) {
            edi.emit('error', new Error('Quoted field not terminated'));
        } else {
            // dump open record
            if (state.field) {
                if(edi.readOptions.trim || edi.readOptions.rtrim){
                    state.field = state.field.trimRight();
                }
                state.line.push(state.field);
                state.field = '';
            }
            if (state.line.length > 0) {
                flush();
            }
            if(edi.writeStream){
                if(state.bufferPosition !== 0){
                    edi.writeStream.write(state.buffer.slice(0, state.bufferPosition));
                }
                if(this.writeOptions.end){
                    edi.writeStream.end();
                }else{
                    edi.emit('end', state.count);
                    edi.readable = false;
                }
            }else{
                edi.emit('end', state.count);
                edi.readable = false;
            }
        }
    }
    
    EDI.prototype.toStream = function(writeStream, options){
        if(options) merge(this.writeOptions,options);
        var self = this;
        switch(this.writeOptions.lineBreaks){
            case 'auto':
                this.writeOptions.lineBreaks = null;
                break;
            case 'unix':
                this.writeOptions.lineBreaks = "\n";
                break;
            case 'mac':
                this.writeOptions.lineBreaks = "\r";
                break;
            case 'windows':
                this.writeOptions.lineBreaks = "\r\n";
                break;
            case 'unicode':
                this.writeOptions.lineBreaks = "\u2028";
                break;
        }
        writeStream.on('close', function(){
            self.emit('end', state.count);
            self.readable = false;
            self.writable = false;
        })
        this.writeStream = writeStream;
        state.buffer = new Buffer(this.writeOptions.bufferSize||this.readOptions.bufferSize);
        state.bufferPosition = 0;
        return this;
    }
    
    EDI.prototype.toPath = function(path, options){
        // Merge user provided options
        if(options) merge(this.writeOptions,options);
        // clone options
        var options = merge({},this.writeOptions);
        // Delete end property which otherwise overwrite `WriteStream.end()`
        delete options.end;
        // Create the write stream
        var stream = fs.createWriteStream(path, options);
        return this.toStream(stream, null);
    }
    
    // Transform API
    
    EDI.prototype.transform = function(callback){
        this.transformer = callback;
        return this;
    }
    
    var edi = new EDI();
    
    // Private API
    
    /**
     * Parse a string which may hold multiple lines.
     * Private state object is enriched on each character until 
     * flush is called on a new line
     */
    function parse(chars){
        chars = '' + chars;
        for (var i = 0, l = chars.length; i < l; i++) {
            var c = chars.charAt(i);
            switch (c) {
                case edi.readOptions.escape:
                case edi.readOptions.quote:
                    if( state.commented ) break;
                    var isEscape = false;
                    if (c === edi.readOptions.escape) {
                        // Make sure the escape is really here for escaping:
                        // if escape is same as quote, and escape is first char of a field and it's not quoted, then it is a quote
                        // next char should be an escape or a quote
                        var nextChar = chars.charAt(i + 1);
                        if( !( edi.readOptions.escape === edi.readOptions.quote && !state.field && !state.quoted )
                        &&   ( nextChar === edi.readOptions.escape || nextChar === edi.readOptions.quote ) ) {
                            i++;
                            isEscape = true;
                            c = chars.charAt(i);
                            state.field += c;
                        }
                    }
                    if (!isEscape && (c === edi.readOptions.quote)) {
                        if (state.field && !state.quoted) {
                            // Treat quote as a regular character
                            state.field += c;
                            break;
                        }
                        if (state.quoted) {
                            // Make sure a closing quote is followed by a delimiter
                            var nextChar = chars.charAt(i + 1);
                            if (nextChar && nextChar != '\r' && nextChar != '\n' && nextChar !== edi.readOptions.delimiter) {
                                throw new Error('Invalid closing quote; found "' + nextChar + '" instead of delimiter "' + edi.readOptions.delimiter + '"');
                            }
                            state.quoted = false;
                        } else if (state.field === '') {
                            state.quoted = true;
                        }
                    }
                    break;
                case edi.readOptions.delimiter:
                    if( state.commented ) break;
                    if( state.quoted ) {
                        state.field += c;
                    }else{
                        if(edi.readOptions.trim || edi.readOptions.rtrim){
                            state.field = state.field.trimRight();
                        }
                        state.line.push(state.field);
                        state.field = '';
                    }
                    break;
                case '\n':
                    if(state.quoted) {
                        state.field += c;
                        break;
                    }
                    if( !edi.readOptions.quoted && state.lastC === '\r' ){
                        break;
                    }
                case '\r':
                    if(state.quoted) {
                        state.field += c;
                        break;
                    }
                    if( edi.writeOptions.lineBreaks === null ){
                        // Auto-discovery of linebreaks
                        edi.writeOptions.lineBreaks = c + ( c === '\r' && chars.charAt(i+1) === '\n' ? '\n' : '' );
                    }
                    if(edi.readOptions.trim || edi.readOptions.rtrim){
                        state.field = state.field.trimRight();
                    }
                    state.line.push(state.field);
                    state.field = '';
                    flush();
                    break;
                case ' ':
                case '\t':
                    if(state.quoted || (!edi.readOptions.trim && !edi.readOptions.ltrim ) || state.field) {
                        state.field += c;
                        break;
                    }
                    break;
                default:
                    if(state.commented) break;
                    state.field += c;
            }
            state.lastC = c;
        }
    }
    
    /**
     * Called by the `parse` function on each line. It is responsible for 
     * transforming the data and finally calling `write`.
     */
    function flush(){
        if(edi.readOptions.columns){
            if(state.count === 0 && edi.readOptions.columns === true){
                edi.readOptions.columns = state.line;
                state.line = [];
                state.lastC = '';
                return;
            }
            var line = {};
            for(var i=0; i<edi.readOptions.columns.length; i++){
                var column = edi.readOptions.columns[i];
                line[column] = state.line[i]||null;
            }
            state.line = line;
            line = null;
        }
        var line;
        if(edi.transformer){
            transforming = true;
            try{
                line = edi.transformer(state.line, state.count);
            }catch(e){
                error(e);
                // edi.emit('error', e);
                // // Destroy the input stream
                // if(edi.readStream) edi.readStream.destroy();
            }
            
            if (edi.writeOptions.newColumns && !edi.writeOptions.columns && typeof line === 'object' && !Array.isArray(line)) {
                Object.keys(line)
                .filter(function(column) { return edi.readOptions.columns.indexOf(column) === -1; })
                .forEach(function(column) { edi.readOptions.columns.push(column); });
            }

            transforming = false;
        }else{
            line = state.line;
        }
        if(state.count === 0 && edi.writeOptions.header === true){
            write(edi.writeOptions.columns || edi.readOptions.columns);
        }
        write(line);
        state.count++;
        state.line = [];
        state.lastC = '';
    }
    
    /**
     * Write a line to the written stream.
     * Line may be an object, an array or a string
     * Preserve is for line which are not considered as EDI data
     */
    function write(line, preserve){
        if(typeof line === 'undefined' || line === null){
            return;
        }
        if(!preserve){
            try {
                edi.emit('data', line, state.count);
            }catch(e){
                error(e);
                // edi.emit('error', e);
                // edi.readable = false;
                // edi.writable = false;
            }
        }
        if(typeof line === 'object'){
            if(!(line instanceof Array)){
                var columns = edi.writeOptions.columns || edi.readOptions.columns;
                var _line = [];
                if(columns){
                    for(var i=0; i<columns.length; i++){
                        var column = columns[i];
                        _line[i] = (typeof line[column] === 'undefined' || line[column] === null) ? '' : line[column];
                    }
                }else{
                    for(var column in line){
                        _line.push(line[column]);
                    }
                }
                line = _line;
                _line = null;
            }else if(edi.writeOptions.columns){
                // We are getting an array but the user want specified output columns. In
                // this case, we respect the columns indexes
                line.splice(edi.writeOptions.columns.length);
            }
            if(line instanceof Array){
                var newLine = state.countWriten ? edi.writeOptions.lineBreaks || "\n" : '';
                for(var i=0; i<line.length; i++){
                    var field = line[i];
                    if(typeof field === 'string'){
                        // fine 99% of the cases, keep going
                    }else if(typeof field === 'number'){
                        // Cast number to string
                        field = '' + field;
                    }else if(typeof field === 'boolean'){
                        // Cast boolean to string
                        field = field ? '1' : '';
                    }else if(field instanceof Date){
                        // Cast date to timestamp string
                        field = '' + field.getTime();
                    }
                    if(field){
                        var containsdelimiter = field.indexOf(edi.writeOptions.delimiter || edi.readOptions.delimiter) >= 0;
                        var containsQuote = field.indexOf(edi.writeOptions.quote || edi.readOptions.quote) >= 0;
                        var containsLinebreak = field.indexOf("\r") >= 0 || field.indexOf("\n") >= 0;
                        if(containsQuote){
                            field = field.replace(
                                    new RegExp(edi.writeOptions.quote || edi.readOptions.quote,'g')
                                  , (edi.writeOptions.escape || edi.readOptions.escape)
                                  + (edi.writeOptions.quote || edi.readOptions.quote));
                        }
                        if(containsQuote || containsdelimiter || containsLinebreak){
                            field = (edi.writeOptions.quote || edi.readOptions.quote) + field + (edi.writeOptions.quote || edi.readOptions.quote);
                        }
                        newLine += field;
                    }
                    if(i!==line.length-1){
                        newLine += edi.writeOptions.delimiter || edi.readOptions.delimiter;
                    }
                }
                line = newLine;
            }
        }else if(typeof line == 'number'){
            line = ''+line;
        }
        if(state.buffer){
            if(state.bufferPosition + Buffer.byteLength(line, edi.writeOptions.encoding) > edi.readOptions.bufferSize){
                edi.writeStream.write(state.buffer.slice(0, state.bufferPosition));
                state.buffer = new Buffer(edi.readOptions.bufferSize);
                state.bufferPosition = 0;
            }
            state.bufferPosition += state.buffer.write(line, state.bufferPosition, edi.writeOptions.encoding);
        }
        if(!preserve){
            state.countWriten++;
        }
        return true;
    }

    function error(e){
        edi.emit('error', e);
        edi.readable = false;
        edi.writable = false;
        // Destroy the input stream
        if(edi.readStream) edi.readStream.destroy();
    }
    
    return edi;
};
