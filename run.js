var parser = require("csv-parse/sync");
var stringifier = require("csv-stringify/sync");
var fs = require("fs");

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

//ftp://ftp.ncbi.nlm.nih.gov/hapmap/recombination/2011-01_phaseII_B37/
var cM = {};
console.log("Loading HapMap II GRCh37");
for(let i = 1; i <= 22; i++){
	cM[i] = parser.parse(fs.readFileSync('genetic_map_HapMapII_GRCh37/genetic_map_GRCh37_chr' + i + '.txt').toString(), {columns: true, skip_empty_lines: true, delimiter: "\t"})
}

console.log("Loading Input Files");
for(let i = 0; i < files.length; i++){
	file_content.push(readRawGene(files[i]));
}

let data = {};
let backbonedData = {};
let overlappedData = [];
let summary = [];

file_content[0].table.forEach((item) => {
	backbonedData[item.RSID] = item;
});

for(let i = 0; i < files.length; i++){
	for(let j = i + 1; j < files.length; j++){
		console.log("Processing " + files[i] + " ∩ " + files[j]);
		let result = mergeIntersection(file_content[i], file_content[j]);
		summary.push({args: process.argv, source: files[i] + " ∩ " + files[j], summary: result.summary});
		Object.assign(data, result.data);
		Object.assign(backbonedData, result.data);
		overlappedData = overlappedData.concat(Object.values(result.data));
	}
}

console.log("Packing Final Result");

let arr = Object.values(data).sort((a, b) => {
	return a.CHROMOSOME == b.CHROMOSOME ? a.POSITION - b.POSITION : a.CHROMOSOME - b.CHROMOSOME;
});
let arr2 = Object.values(backbonedData).sort((a, b) => {
	return a.CHROMOSOME == b.CHROMOSOME ? a.POSITION - b.POSITION : a.CHROMOSOME - b.CHROMOSOME;
});

fs.writeFileSync(output_prefix + "-hybrid-output-" + timestamp + ".csv", stringifier.stringify(arr, {header: true}));
fs.writeFileSync(output_prefix + "-backboned-hybrid-output-" + timestamp + ".csv", stringifier.stringify(arr2, {header: true}));
fs.writeFileSync(output_prefix + "-overlapped-hybrid-output-" + timestamp + ".csv", stringifier.stringify(overlappedData, {header: true}));
fs.writeFileSync(output_prefix + "-summary-" + timestamp + ".json", JSON.stringify(summary, null, 2));



function searchGRCh34Pos(chr, pos, i = -1, j = -2){
	if(i == -1){
		return searchGRCh34Pos(chr, pos, 0, cM[chr].length);
	}else if(i == j){
		return i == cM[chr].length ? cM[chr].length - 1 : i;
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
	return parseFloat(cM[chr][searchGRCh34Pos(chr, endPos)]['Map(cM)']) - parseFloat(cM[chr][searchGRCh34Pos(chr, startPos)]['Map(cM)']);;
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
		if(["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22"].indexOf(item.CHROMOSOME) > -1){
			obj.rsidmap[item.RSID] = {rsid: item.RSID, chromosome: parseInt(item.CHROMOSOME), pos: parseInt(item.POSITION), result: item.RESULT};
		}
	});
	obj.arr = Object.values(obj.rsidmap).sort((a, b) => {
		return a.chromosome == b.chromosome ? a.pos - b.pos : a.chromosome - b.chromosome;
	})
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
	console.log("Compare");
	let res = compare(a, b);
	console.log("Matching");
	let table = genMatchResult(res);
	let toCM = [];
	table.forEach((item) => {
		if(item.cm >= cM_threshold && item.snp >= minimum_snps){
			toCM.push(item);
		}
	})
	let result = {};
	console.log("Extracting");
	let cursor = 0;
	toCM.forEach((range) => {
		let flag = false;
		while(cursor < a.arr.length){
			aSNP = a.arr[cursor];
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
	/*toCM.forEach((range) => {
		Object.values(a.rsidmap).forEach((aSNP) => {
			if(aSNP.chromosome == range.chr && aSNP.pos >= range.start && aSNP.pos <= range.end){
				result[aSNP.rsid] = {RSID: aSNP.rsid, CHROMOSOME: aSNP.chromosome.toString(), POSITION: aSNP.pos.toString(), RESULT: aSNP.result};
			}
		});
	});*/
	console.log("Done");
	return {summary: toCM, data: result};
}