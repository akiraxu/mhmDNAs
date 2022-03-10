var parser = require("csv-parse/sync");
var stringifier = require("csv-stringify/sync");
var fs = require("fs");
var path = require('path');
var crypto = require("crypto");
//var formidable = require('formidable');
//var http = require('http');

var MAXCONCURRENT = 1;
var CURRENTCONCURRENT = 0;

var formOpt = {uploadDir: `${__dirname}/uploads`, maxFileSize: 200 * 1024 * 1024, multiples:true};

/*
if(process.argv.length < 7){
	console.log("Usage: node run [cM] [SNPs] [prefix] [file1] [file2] [file3] ...")
	console.log("E.g.: node run 2.5 50 output-2020 aaa.csv bbb.csv ccc.csv")
	process.exit(1);
}
var files = process.argv.slice(5);
var cM_threshold = parseFloat(process.argv[2]);
var minimum_snps = parseInt(process.argv[3]);
var output_prefix = process.argv[4];
var file_content = [];
var timestamp = Date.now();
*/

setInterval(()=>{console.log((new Date).toISOString())}, 100)

//ftp://ftp.ncbi.nlm.nih.gov/hapmap/recombination/2011-01_phaseII_B37/
var cM = {};
console.log("Loading HapMap II GRCh37");
for(let i = 1; i <= 22; i++){
	cM[i] = parser.parse(fs.readFileSync('genetic_map_HapMapII_GRCh37/genetic_map_GRCh37_chr' + i + '.txt').toString(), {columns: true, skip_empty_lines: true, delimiter: "\t"})
}

class MhmDNAs {

	constructor(cm, file_path_arr, cm_str, snp_str, prefix, fc = []){
		this.cM = cm;
		this.files = file_path_arr;
		this.cM_threshold = parseFloat(cm_str);
		this.minimum_snps = parseInt(snp_str);
		this.output_prefix = prefix;
		this.file_content = fc;
		this.timestamp = Date.now();
		this.id = crypto.randomBytes(20).toString('hex');
		this.load_file_counter = 0;
	}
	
	doit(){
		setTimeout(function(){
			this.loadfiles(this.processing.bind(this));
		}.bind(this), 100);
		return this.id;
	}
	
	waitit(cond, cb){
		if(cond()){
			cb();
		}else{
			setTimeout(function(){this.waitit(cond, cb);}.bind(this), 1000);
		}
	}

	loadfiles(cb){
		console.log("Loading Input Files");
		for(let i = 0; i < this.files.length; i++){
			setImmediate(function(){
				if(this.file_content.length == 0){
					this.readRawGeneFn(this.files[i], (obj) => {
						this.file_content.push(obj);
						this.load_file_counter ++;
					});
				}else{
					this.readRawGene(this.file_content[i], (obj) => {
						this.file_content.push(obj);
						this.load_file_counter ++;
					});
				}
			}.bind(this));
		}
		this.waitit(() => {
			return this.load_file_counter == this.files.length;
		}, () => {
			cb();
		});
	}

	processing(){
		let data = {};
		let backbonedData = {};
		let overlappedData = [];
		let summary = [];
		
		//let counter = 0;
		//let done = 0;

		this.file_content[0].table.forEach((item) => {
			backbonedData[item.RSID] = item;
		});

		for(let i = 0; i < this.files.length; i++){
			for(let j = i + 1; j < this.files.length; j++){
				console.log("Processing " + this.files[i] + " ∩ " + this.files[j]);
				//counter ++;
				//setImmediate(function(){
					this.mergeIntersection(this.file_content[i], this.file_content[j], (result) => {
						summary.push({args: {cM_threshold: this.cM_threshold, minimum_snps: this.minimum_snps}, source: path.basename(this.files[i]) + " ∩ " + path.basename(this.files[j]), summary: result.summary});
						Object.assign(data, result.data);
						Object.assign(backbonedData, result.data);
						overlappedData = overlappedData.concat(Object.values(result.data));
						//done ++;
					});
				//}.bind(this));
			}
		}
		
		//this.waitit(() => {
		//	return counter == done;
		//}, () => {
			let arr = Object.values(data).sort((a, b) => {
				return a.CHROMOSOME == b.CHROMOSOME ? a.POSITION - b.POSITION : a.CHROMOSOME - b.CHROMOSOME;
			});
			let arr2 = Object.values(backbonedData).sort((a, b) => {
				return a.CHROMOSOME == b.CHROMOSOME ? a.POSITION - b.POSITION : a.CHROMOSOME - b.CHROMOSOME;
			});
			console.log("Packing Final Result");
			fs.writeFile(this.output_prefix + "-hybrid-output-" + this.timestamp + ".csv", stringifier.stringify(arr, {header: true}), () => {});
			fs.writeFile(this.output_prefix + "-backboned-hybrid-output-" + this.timestamp + ".csv", stringifier.stringify(arr2, {header: true}), () => {});
			fs.writeFile(this.output_prefix + "-overlapped-hybrid-output-" + this.timestamp + ".csv", stringifier.stringify(overlappedData, {header: true}), () => {});
			fs.writeFile(this.output_prefix + "-summary-" + this.timestamp + ".json", JSON.stringify(summary, null, 2), () => {});
		//});
	}

	searchGRCh34Pos(chr, pos, i = -1, j = -2){
		if(i == -1){
			return this.searchGRCh34Pos(chr, pos, 0, this.cM[chr].length);
		}else if(i == j){
			return i == this.cM[chr].length ? this.cM[chr].length - 1 : i;
		}else{
			let mid = Math.floor((j - i) / 2) + i;
			let value = parseFloat(this.cM[chr][mid]['Position(bp)']);
			if(value == pos){
				return mid;
			}else if(value > pos){
				return this.searchGRCh34Pos(chr, pos, i, mid);
			}else{
				return this.searchGRCh34Pos(chr, pos, mid + 1, j);
			}
		}
	}

	calculateCM(chr, startPos, endPos){
		return parseFloat(this.cM[chr][this.searchGRCh34Pos(chr, endPos)]['Map(cM)']) - parseFloat(this.cM[chr][this.searchGRCh34Pos(chr, startPos)]['Map(cM)']);;
	}

	calcCM(obj){
		return parseFloat(this.cM[obj.chr][this.searchGRCh34Pos(obj.chr, obj.end)]['Map(cM)']) - parseFloat(this.cM[obj.chr][this.searchGRCh34Pos(obj.chr, obj.start)]['Map(cM)'])
	}

	readRawGene(data, cb){
		let obj = {};
		let table = parser.parse(data.toString(),{columns: true, skip_empty_lines: true});
		obj.table = table;
		obj.rsidmap = {};
		table.forEach((item) => {
			if(["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22"].indexOf(item.CHROMOSOME) > -1){
				obj.rsidmap[item.RSID] = {rsid: item.RSID, chromosome: parseInt(item.CHROMOSOME), pos: parseInt(item.POSITION), result: item.RESULT};
			}
		});
		obj.arr = Object.values(obj.rsidmap).sort((a, b) => {
			return a.chromosome == b.chromosome ? a.pos - b.pos : a.chromosome - b.chromosome;
		})
		cb(obj);
	}
	
	readRawGeneFn(fn, cb){
		fs.readFile(fn, (err, data) => {
			let obj = {};
			let table = parser.parse(data.toString(),{columns: true, skip_empty_lines: true});
			obj.table = table;
			obj.rsidmap = {};
			table.forEach((item) => {
				if(["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22"].indexOf(item.CHROMOSOME) > -1){
					obj.rsidmap[item.RSID] = {rsid: item.RSID, chromosome: parseInt(item.CHROMOSOME), pos: parseInt(item.POSITION), result: item.RESULT};
				}
			});
			obj.arr = Object.values(obj.rsidmap).sort((a, b) => {
				return a.chromosome == b.chromosome ? a.pos - b.pos : a.chromosome - b.chromosome;
			})
			cb(obj);
		});
	}

	isHalfIdentical(a, b){
		if(!a){
			return {isMatch: true, isOnlyOne: true, rsid: b.rsid, chromosome: b.chromosome, pos: b.pos};
		}
		if(!b){
			return {isMatch: true, isOnlyOne: true, rsid: a.rsid, chromosome: a.chromosome, pos: a.pos};
		}
		if(a.result == '--' || b.result == '--'){
			return {isMatch: true, isOnlyOne: true, rsid: a.rsid, chromosome: a.chromosome, pos: a.pos};
		}
		if(a.rsid == b.rsid && a.chromosome == b.chromosome && a.pos == b.pos){
			let result = a.result[0] == b.result[0] || a.result[0] == b.result[1] || a.result[1] == b.result[0] || a.result[1] == b.result[1];
			//result = a.result[0] == b.result[0] || a.result[1] == b.result[1];
			return {isMatch: result, isOnlyOne: false, rsid: a.rsid, chromosome: a.chromosome, pos: a.pos};
		}
		return {isMatch: false, isOnlyOne: false, rsid: a.rsid, chromosome: a.chromosome, pos: a.pos};
	}

	compareOneWay(a, b, cb){
		let match = {};
		let counter = 0;
		let done = 0;
		Object.keys(a.rsidmap).forEach((rsid) => {
			counter ++;
			setImmediate(function(){
				match[rsid] = this.isHalfIdentical(a.rsidmap[rsid], b.rsidmap[rsid]);
				done ++;
			}.bind(this));
		});
		this.waitit(() => {
			return counter == done;
		}, () => {
			cb(match);
		});
	}

	compare(a, b, cb){
		this.compareOneWay(a, b, (matchA) => {
			this.compareOneWay(b, a, (matchB) => {
				setImmediate(function(){
					cb(Object.assign(matchA, matchB));
				}.bind(this));
			});
		});
	}

	genMatchResult(obj){
		let arr = Object.values(obj).sort((a, b) => {
			return a.chromosome == b.chromosome ? a.pos - b.pos : a.chromosome - b.chromosome;
		});
		let result = [];
		let flag = false;
		let start = 0;
		let prev = 0;
		for(let i = 0; i < arr.length; i++){
			if(arr[prev].chromosome != arr[i].chromosome){
				if(flag){
					result.push({chr: arr[start].chromosome, start: arr[start].pos, end: arr[prev].pos, cm: this.calculateCM(arr[start].chromosome, arr[start].pos, arr[prev].pos), snp: prev - start + 1});
				}
				flag = false;
			}
			if(arr[i].isMatch){
				if(!flag){
					flag = true;
					start = i;
				}
				prev = i;
			}else{
				if(flag){
					flag = false;
					result.push({chr: arr[start].chromosome, start: arr[start].pos, end: arr[prev].pos, cm: this.calculateCM(arr[start].chromosome, arr[start].pos, arr[prev].pos), snp: prev - start + 1});
				}
			}
		}
		return result;
	}

	mergeIntersection(a, b, cb){
		if(CURRENTCONCURRENT >= MAXCONCURRENT){
			setTimeout(function(){this.mergeIntersection(a, b, cb)}.bind(this), 100)
		}else{
			CURRENTCONCURRENT ++;
			console.log("Compare");
			this.compare(a, b, (res) => {
				console.log("Matching");
				let table = this.genMatchResult(res);
				let toCM = [];
				table.forEach((item) => {
					if(item.cm >= this.cM_threshold && item.snp >= this.minimum_snps){
						toCM.push(item);
					}
				})
				let result = {};
				console.log("Extracting");
				let cursor = 0;
				toCM.forEach((range) => {
					let flag = false;
					while(cursor < a.arr.length){
						let aSNP = a.arr[cursor];
						if(aSNP.chromosome == range.chr && aSNP.pos >= range.start && aSNP.pos <= range.end){
							result[aSNP.rsid] = {RSID: aSNP.rsid, CHROMOSOME: aSNP.chromosome.toString(), POSITION: aSNP.pos.toString(), RESULT: aSNP.result};
							flag = true;
						}else{
							if(flag){
								break;
							}
						}
						cursor++;
					}
					
				});
				console.log("Done");
				CURRENTCONCURRENT --;
				cb({summary: toCM, data: result});
			});
		}
		/*toCM.forEach((range) => {
			Object.values(a.rsidmap).forEach((aSNP) => {
				if(aSNP.chromosome == range.chr && aSNP.pos >= range.start && aSNP.pos <= range.end){
					result[aSNP.rsid] = {RSID: aSNP.rsid, CHROMOSOME: aSNP.chromosome.toString(), POSITION: aSNP.pos.toString(), RESULT: aSNP.result};
				}
			});
		});*/
	}
}

function server(req, res) {
	if(req.method == 'POST'){
		const fm = formidable(formOpt);
		fm.parse(req, (err, fields, files) => {
			if(err){
				console.log(err);
				res.writeHead(err.httpCode || 400, { 'Content-Type': 'text/plain' });
				res.end("Something wrong");
				return;
			}
			if(!files.filetoupload){
				res.writeHead(400, { 'Content-Type': 'text/plain' });
				res.end("Something wrong");
				return;
			}
			let file_arr = Array.isArray(files.filetoupload) ? files.filetoupload : [files.filetoupload];
			let file_data = [];
			file_arr.forEach((aFile) => {
				let data = fs.readFileSync(files.filetoupload.filepath);
				file_data.push({
					fileName,
				});
			});

			fs.readFile(files.filetoupload.filepath, (err, file) => {
				//cmd = "./ipfs.exe add " + newFilename + "tempfile 2>\/dev\/null| tr -d \'\\n\'| tr -d \'\\r\' | sed \'s\/.*added\\s*\\(\\w*\\)\\s*" + rand + "tempfile.*\/\\1\/\'";
				ipfs.add(file).then((data) => {
					let summary = {
						time: new Date().toISOString(),
						ip: ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress || null,
						file: files.filetoupload.originalFilename,
						mime: files.filetoupload.mimetype,
						hash: files.filetoupload.hash,
						cid: data.path,
						size: files.filetoupload.size
					};
					console.log(summary);
					summary.header = req.rawHeaders;
					db.push(summary);
					fs.writeFileSync("db.json", JSON.stringify(db, null, 2));
					fs.unlink(files.filetoupload.filepath, (err, file) => {});
					res.writeHead(200, { 'Content-Type': 'text/plain' });
					let result = 'https://' + hosts[Math.floor(Math.random() * hosts.length)] + '/ipfs/' + data.path + '?filename=' + files.filetoupload.originalFilename;
					res.end(result);
					request(result, function(error, response, body){console.log(data.path + " verified")});
				});
			});
		});
	}else{
	res.writeHead(200, {'Content-Type': 'text/html'});
	res.write(fs.readFileSync('index.html'));
	res.end();
  }
}


console.log((new MhmDNAs(cM, process.argv.slice(5), process.argv[2], process.argv[3], process.argv[4])).doit());
























