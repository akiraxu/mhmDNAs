var fs = require("fs");
var path = require('path');
var crypto = require("crypto");
var child_process = require("child_process");
var formidable = require('formidable');
var http = require('http');

var formOpt = {uploadDir: `${__dirname}/uploads`, maxFileSize: 200 * 1024 * 1024, multiples:true};

var db = JSON.parse(fs.readFileSync('db.json', {encoding: "utf8"}));

http.createServer(server).listen(8081);

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
			let file_path_arr = [];
			file_arr.forEach((aFile) => {
				file_path_arr.push(aFile.filepath);
			});
			let obj = {
				id: crypto.randomBytes(16).toString('hex'),
				isDone: false,
				mincm: fields.mincm,
				minsnp: fields.minsnp,
				files: file_path_arr
			};
			db[obj.id] = obj;
			let child = child_process.fork("run.js");
			child.send(obj);
			child.on("message", function(rc){
				console.log("child finish");
				db[obj.id].isDone = true;
				fs.writeFileSync("db.json", JSON.stringify(db, null, 2));
			});
			child.on("close", function(rc){
				console.log("child end");
			});
			res.writeHead(200, {'Content-Type': 'text/plain'});
			res.write(JSON.stringify(obj, null, 2));
			res.end();
		});
	}else{
	res.writeHead(200, {'Content-Type': 'text/html'});
	res.write(fs.readFileSync('index.html'));
	res.end();
  }
}