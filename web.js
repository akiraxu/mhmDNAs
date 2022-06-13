var fs = require("fs");
var path = require('path');
var crypto = require("crypto");
var child_process = require("child_process");
var formidable = require('formidable');
var http = require('http');
var MUTEX_LOCK = false; 

var formOpt = {uploadDir: `${__dirname}/uploads`, maxFileSize: 1024 * 1024 * 1024, multiples:true};

var db = JSON.parse(fs.readFileSync('db.json', {encoding: "utf8"}));

var config = JSON.parse(fs.readFileSync('config.json', {encoding: "utf8"}));

var twilioClient = require('twilio')(config.twilio.accountSid, config.twilio.authToken);
var request = require('request');
var querystring = require("querystring");

http.createServer(server).listen(8080);

function queueJob(obj){
	if(!MUTEX_LOCK){
		MUTEX_LOCK = true;
		obj.status = "running";
		let child = child_process.fork("run.js");
		child.send(obj);
		child.on("message", function(rc){
			console.log("child finish");
			db[obj.id].isDone = true;
			fs.writeFileSync("db.json", JSON.stringify(db, null, 2));
			stnr('https://www.ybackmachine.com/' + obj.id, (url) => {
				sendSMS(db[obj.id].phone, "Your Y-back Machine job " + obj.id + " is done at: " + url);
			});
		});
		child.on("close", function(rc){
			console.log("child end");
			MUTEX_LOCK = false;
			obj.status = db[obj.id].isDone ? "done" : "error";
		});
	}else{
		setTimeout(()=>{queueJob(obj);}, 500);
	}
};

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
			if(!files.filetoupload0){
				res.writeHead(400, { 'Content-Type': 'text/plain' });
				res.end("Something wrong");
				return;
			}
			
			let file_path_arr = [];
			let file_orig_name_arr = [];
			let file_group = [];
			
			let i = 0;
			while(files['filetoupload' + i]){
				let file_arr = Array.isArray(files['filetoupload' + i]) ? files['filetoupload' + i] : [files['filetoupload' + i]];
				file_arr.forEach((aFile) => {
					if(aFile.size > 0){
						file_path_arr.push(aFile.filepath);
						file_orig_name_arr.push(aFile.originalFilename);
						file_group.push(i);
					}
				});
				i++;
			}
			
			let obj = {
				id: crypto.randomBytes(16).toString('hex'),
				isDone: false,
				mincm: fields.mincm,
				minsnp: fields.minsnp,
				phone: fields.phone,
				files: file_path_arr,
				origfn: file_orig_name_arr,
				group: file_group,
				diploid: fields.diploid == "on",
				unify: fields.unify == "on",
				status: "queued"
			};
			db[obj.id] = obj;
			queueJob(obj);
			res.writeHead(302, {'Location': '/' + obj.id});
			res.end();
		});
	}else if(req.method == 'GET' && req.url !='/'){
		let token = (/^\/([a-z0-9]{32})/).exec(req.url);
		console.log(req.url);
		if(token){
			let fn = (/^\/([a-z0-9]{32}-[a-z0-9-]+\.(csv|json))/).exec(req.url);
			if(fn){
				if(fs.existsSync(fn[1])){
					res.writeHead(200, {
						'Content-Type': 'application/octet-stream',
						'Content-Disposition': 'attachment;filename="' + fn[1] + '"'
					});
					res.write(fs.readFileSync(fn[1]));
					res.end();
				}else{
					res.writeHead(404, {'Content-Type': 'text/html'});
					res.end();
				}
			}else{
				let results = fs.readdirSync("./").filter(fn => fn.includes(token[1]));
				let html = "<h1>Job " + token[1] + " status : " + (db[token[1]] ? db[token[1]].status : "invalid") + "</h1></br>";
				html += "<h1>Below are the results, if not shown, bookmark this page and check later</h1></br>";
				results.forEach((item) => {
					html += '<a href="/' + item + '">' + item + '</a></br>';
				});
				res.writeHead(200, {'Content-Type': 'text/html'});
				res.write(html);
				res.end();
			}
		}else{
			res.writeHead(404, {'Content-Type': 'text/html'});
			res.end();
		}
	}else{
	res.writeHead(200, {'Content-Type': 'text/html'});
	res.write(fs.readFileSync('index.html'));
	res.end();
  }
}

function stnr(url, cb){
	qstr = querystring.stringify({url: encodeURIComponent(Buffer.from(encodeURI(url)).toString('base64')), host: 'stnr.icu'});
	requrl = 'https://stnr.icu/shortener?' + qstr;
	request({url: requrl, method: "POST", body: qstr}, (err, res, body)=>{
		cb(body.split("|")[0])
	})
}

function sendSMS(number, msg){
	if(!(/^([0-9]{10})$/).exec(number)){
		return;
	}
	twilioClient.messages.create({
		body: msg,
		messagingServiceSid: config.twilio.messagingServiceSid,
		to: number
	}).then(message => console.log(message.sid)).done();
}