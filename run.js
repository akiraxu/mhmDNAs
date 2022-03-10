var parser = require("csv-parse/sync");
var stringifier = require("csv-stringify/sync");
var fs = require("fs");

if(process.argv.length < 5){
	console.log("Usage: node run [cM] [file1] [file2] [file3] ...")
	process.exit(1);
}
var files = process.argv.slice(3);
var cM_threshold = parseFloat(process.argv[2]);
var file_content = [];

//ftp://ftp.ncbi.nlm.nih.gov/hapmap/recombination/2011-01_phaseII_B37/
var cM = {};
for(let i = 1; i <= 22; i++){
	cM[i] = parser.parse(fs.readFileSync('genetic_map_HapMapII_GRCh37/genetic_map_GRCh37_chr1.txt').toString(), {columns: true, skip_empty_lines: true, delimiter: "\t"})
}

for(let i = 0; i < files.length; i++){
	file_content.push(readRawGene(files[i]));
}

let data = {};
let summary = [];

for(let i = 0; i < files.length; i++){
	for(let j = i + 1; j < files.length; j++){
		let result = mergeIntersection(file_content[i], file_content[i]);
		summary.push({source: files[i] + " ∩ " + files[j], summary: result.summary});
		Object.assign(data, result.data);
	}
}
let arr = Object.values(data).sort((a, b) => {
	return a.chromosome == b.chromosome ? a.pos - b.pos : a.chromosome - b.chromosome;
});

fs.writeFileSync("result.csv", stringifier.stringify(arr, {header: true}));
fs.writeFileSync("summary.json", JSON.stringify(summary, null, 2));



function searchGRCh34Pos(chr, pos, i = -1, j = -2){
	if(i == -1){
		return searchGRCh34Pos(chr, pos, 0, cM[chr].length);
	}else if(i == j){
		return i;
	}else{
		let mid = Math.floor((j - i) / 2) + i;
		let value = parseFloat(cM[chr][mid]['Position(bp)']);
		if(value == pos){
			return mid;
		}else if(value > pos){
			return searchGRCh34Pos(chr, pos, i, mid);
		}else{
			return searchGRCh34Pos(chr, pos, mid + 1, j);
		}
	}
}

function calculateCM(chr, startPos, endPos){
	return parseFloat(cM[chr][searchGRCh34Pos(chr, endPos)]['Map(cM)']) - parseFloat(cM[chr][searchGRCh34Pos(chr, startPos)]['Map(cM)'])
}

function calcCM(obj){
	return parseFloat(cM[obj.chr][searchGRCh34Pos(obj.chr, obj.end)]['Map(cM)']) - parseFloat(cM[obj.chr][searchGRCh34Pos(obj.chr, obj.start)]['Map(cM)'])
}

function readRawGene(fn){
	let obj = {};
	let file = fs.readFileSync(fn).toString();
	let table = parser.parse(file,{columns: true, skip_empty_lines: true});
	obj.table = table;
	obj.rsidmap = {};
	table.forEach((item) => {
		obj.rsidmap[item.RSID] = {rsid: item.RSID, chromosome: parseInt(item.CHROMOSOME), pos: parseInt(item.POSITION), result: item.RESULT};
	});
	return obj;
}

function isHalfIdentical(a, b){
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

function compareOneWay(a, b){
	let match = {};
	Object.keys(a.rsidmap).forEach((rsid) => {
		match[rsid] = isHalfIdentical(a.rsidmap[rsid], b.rsidmap[rsid])
	});
	return match;
}

function compare(a, b){
	return Object.assign(compareOneWay(a, b), compareOneWay(b, a));
}

function genMatchResult(obj){
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
				result.push({chr: arr[start].chromosome, start: arr[start].pos, end: arr[prev].pos, cm: calculateCM(arr[start].chromosome, arr[start].pos, arr[prev].pos), snp: prev - start + 1});
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
				result.push({chr: arr[start].chromosome, start: arr[start].pos, end: arr[prev].pos, cm: calculateCM(arr[start].chromosome, arr[start].pos, arr[prev].pos), snp: prev - start + 1});
			}
		}
	}
	return result;
}

function mergeIntersection(a, b){
	let res = compare(a, b);
	let table = genMatchResult(res);
	let toCM = [];
	table.forEach((item) => {
		if(item.cm > cM_threshold){
			toCM.push(item);
		}
	})
	let result = {};
	toCM.forEach((range) => {
		Object.values(a.rsidmap).forEach((aSNP) => {
			if(aSNP.chromosome == range.chr && aSNP.pos >= range.start && aSNP.pos <= range.end){
				result[aSNP.rsid] = {rsid: aSNP.rsid, chromosome: aSNP.chromosome, pos: aSNP.pos, result: aSNP.result};
			}
		});
	});
	return {summary: toCM, data: result};
}