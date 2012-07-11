// Module EDI - Copyright Rinie Kervel (BSD Licensed)
// Based on Module CSV Copyright David Worms <open@adaltas.com> (BSD Licensed)
// http://en.wikipedia.org/wiki/EDIFACT
/*
UNA:+.? '
UNB+IATB:1+6XPPC+LHPPC+940101:0950+1'
UNH+1+PAORES:93:1:IA'
MSG+1:45'
IFT+3+XYZCOMPANY AVAILABILITY'
ERC+A7V:1:AMD'
IFT+3+NO MORE FLIGHTS'
ODI'
TVL+240493:1000::1220+FRA+JFK+DL+400+C'
PDI++C:3+Y::3+F::1'
APD+74C:0:::6++++++6X'
TVL+240493:1740::2030+JFK+MIA+DL+081+C'
PDI++C:4'
APD+EM2:0:1630::6+++++++DA'
UNT+13+1'
UNZ+1+1'

The UNA segment is optional. If present, it specifies the special characters that are to be used to interpret the remainder of the message. There are six characters following UNA in this order:
component data element separator (: in this sample)
data element separator (+ in this sample)
decimal notification (. in this sample)
release character (? in this sample)
reserved, must be a space
segment terminator (' in this sample)
The special characters in the sample UNA segment above are also the default values.

componentDataElementSeparator csv delimiter
dataElementSeparator csv delimiter
decimalNotation
releaseCharacter csv escape
reserved
segmentTerminator newline

*/

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
        una: false,
        unb:false,
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
            componentDataElementSeparator: ':',
            dataElementSeparator: '+',
            decimalNotation: '.',
            releaseCharacter: '?',
            reserved: ' ',
            segmentTerminator: '\'',
            quote: null, // not used in EDI
            columns: null,
            flags: 'r',
            encoding: 'utf8',
            bufferSize: 8 * 1024 * 1024,
            trim: false,
            ltrim: false,
            rtrim: false
        };
        this.writeOptions = {
            componentDataElementSeparator: null,
            dataElementSeparator:  null,
            decimalNotation: null,
            releaseCharacter: null,
            reserved: null,
            segmentTerminator: "\r\n",
            quote: null, // not used in EDI
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
    	var i = 0;
        chars = '' + chars;
        if (state.una === false && state.unb == false) { //skip until UNA of UNB found
	    for (l = chars.length; i < l; i++) {
	    	var c = chars.charAt(i);
	    	if ((c === 'U') && (chars.charAt(i+1) === 'N') &&  ((chars.charAt(i+2) === 'A') || (chars.charAt(i+2) === 'B'))) {
	    		state.una = (chars.charAt(i+2) === 'A');
	    		state.unb = (chars.charAt(i+2) === 'B');
	    		if (state.una) {
	    			edi.readOptions.componentDataElementSeparator = chars.charAt(i+3); // :
	    			edi.readOptions.dataElementSeparator = chars.charAt(i+4); // '+',
				edi.readOptions.decimalNotation= chars.charAt(i+5); // '.',
				edi.readOptions.releaseCharacter= chars.charAt(i+6); //  '?',
				edi.readOptions.reserved= chars.charAt(i+7); //  ' ',
				edi.readOptions.segmentTerminator= chars.charAt(i+8); //  '\'',
				i += 8;
	    		}
	    		else {
	    			i = 0; 
	    		}
	    		break;
	    	}
	    }
        }
        for (l = chars.length; i < l; i++) {
            var c = chars.charAt(i);
            switch (c) {
                case edi.readOptions.releaseCharacter:
                case edi.readOptions.quote:
                    if( state.commented ) break;
                    var isEscape = false;
                    if (c === edi.readOptions.releaseCharacter) {
                        // Make sure the releaseCharacter is really here for escaping:
                        // if releaseCharacter is same as quote, and releaseCharacter is first char of a field and it's not quoted, then it is a quote
                        // next char should be an releaseCharacter or a quote
                        var nextChar = chars.charAt(i + 1);
                            i++;
                            isEscape = true;
                            c = chars.charAt(i);
                            state.field += c;
                     }
                    break;
		case edi.readOptions.componentDataElementSeparator:
		case edi.readOptions.dataElementSeparator:
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
                case edi.readOptions.segmentTerminator:
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
                    flush(); // line/Segment end so flush
                    break;
                case '\n':
                case '\r':
                //case ' ':
                //case '\t':
                //    if(state.quoted || (!edi.readOptions.trim && !edi.readOptions.ltrim ) || state.field) {
                //        state.field += c;
                //        break;
                //    }
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
                        var containsdelimiter = field.indexOf(edi.writeOptions.dataElementSeparator || edi.readOptions.dataElementSeparator) >= 0;
                        var containsQuote = field.indexOf(edi.writeOptions.quote || edi.readOptions.quote) >= 0;
                        var containsLinebreak = field.indexOf("\r") >= 0 || field.indexOf("\n") >= 0;
                        if(containsQuote){
                            field = field.replace(
                                    new RegExp(edi.writeOptions.quote || edi.readOptions.quote,'g')
                                  , (edi.writeOptions.releaseCharacter || edi.readOptions.releaseCharacter)
                                  + (edi.writeOptions.quote || edi.readOptions.quote));
                        }
                        if(containsQuote || containsdelimiter || containsLinebreak){
                            field = (edi.writeOptions.quote || edi.readOptions.quote) + field + (edi.writeOptions.quote || edi.readOptions.quote);
                        }
                        newLine += field;
                    }
                    if(i!==line.length-1){
                        newLine += edi.writeOptions.dataElementSeparator || edi.readOptions.dataElementSeparator;
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
