import path from 'path';
import fs from 'fs';
import MySQL from './MySQL';
import Mongo from './Mongo';
import tv4 from 'tv4';

/**
 * USAGE : 
 * 
 * let configs = {
 * 
 * 	adapter : 'mysql',//values possible => mysql, mongo
 * 	host : 'localhost',
 * 	port : '3306',
 * 	user : 'username',
 * 	password: 'pass', 	
 * 	db: 'databaseName',
 * 	schemaDir : '<directory which contain the json schema>',
 * 	connectionLimit : 20, //optional
 * 	migrations : 'alter' //optional. values possible => create, alter. defaults to create
 * 		
 * }
 *
 *
 * import Orm from 'Orm';
 * let orm = new Orm(configs);
 * let user = {firstName : 'gayan', lastName : 'Madhumal'};
 * orm.user.create(user);
 */
export default class Orm {

	constructor(configs) {

		this.schema = [];
		this.schemaDir = configs.schemaDir;
		this._setAdapter(configs);

		if(!configs.migrations)
			configs.migrations = 'create';

		switch(configs.migrations){
			case 'create' :
				this._createTableStructure();
				break;
			case 'alter' :
				this._alterTableStructure();
				break;
			case 'default' : 
				throw Error('unsupported migration type.');
				break;
		}

		this._loadSchema(configs.schemaDir);
	}

	/**
	 * find an object from the database using options
	 * @param  {Object} options - filter data from the table
	 * @return {Promise}
	 */
	find(model, options) {

		return new Promise((resolve, reject) => {

			this.adapter.find(model, options)
			.then(foundData => {
				resolve(foundData);
			})
			.catch((error) => {
				reject(error);
			});
		});
	}

	/**
	 * insert an object to the database
	 * @param  {Object} obj object to be inserted.
	 * @return {[type]}     [description]
	 */
	create(model ,obj) {

		let modelSchema = this.schema[model];

		return new Promise((resolve, reject) => {

			this._validate(obj, modelSchema).then(result=>{
				return (result);
			})
			.then(data=>{
				return this.adapter.create(model, obj);
			})
			.then(result => {
				resolve(result);
			})
			.catch(error => {
				reject(error);
			});
		});
	}

	/**
	 * deletes objects from the database based on constraints.
	 * @param  {string} model table or collection name.
	 * @param  {Object} opts  delete constraints.
	 * @return {Promise}
	 */
	delete(model, opts) {

		let modelSchema = this.schema[model];

		return new Promise((resolve, reject) => {
			this.adapter.delete(model, opts).then(result =>{
				resolve(result);
			})
			.catch(error => {
				reject(error);
			});
		});
	}

	/**
	 * update record, objs in a db table/collection
	 * @param  {string} model     name of the table or collection
	 * @param  {object} updateObj update values
	 * @param  {object} opts      update criteria
	 * @return {Object}           result
	 */
	update(model, updateObj, opts) {

		return new Promise((resolve, reject) =>{
			this.adapter.update(model,updateObj, opts).then(result =>{
				resolve(result);
			})
			.catch(error =>{
				reject(error);
			})
		});
	}

	/**
	 * eg: 
	 *
	 * query = "UPDATE user SET firstName=? WHERE lastName=?";
	 * values = ['gayan', 'witharana'];
	 * orm.query(query, values);
	 * 
	 * can be used for custom queries.
	 * @param  {String} query  sql query
	 * @param  {Array} values place holder values
	 * @return {Object}
	 */
	query(query, values) {

		return new Promise((resolve, reject) =>{
			this.adapter.query(query, values).then(result =>{
				resolve(result);
			})
			.catch(error =>{
				reject(error);
			});
		});
	}

	/**
	 * destroy the connection
	 */
	end() {
		this.adapter.end();
	}

	/**
	 * Set the adapter type acording to the database to be used.
	 * @param {string} adapter currently only 'mysql' and 'mongo' values are supported
	 */
	_setAdapter (configs){

		if(!configs.adapter)
			throw Error('adapter not set.');

		switch(configs.adapter) {
			case 'mysql':
				this.adapter = new MySQL(configs);
				break;
			case 'mongo':
				this.adapter = new Mongo(configs);
				break;
			default : 
				throw Error('specified adapter not supported.');
				break; 
		}
	}

	/**
	 * [_loadSchema description]
	 * @param  {[type]} schemaDir [description]
	 * @return {[type]}           [description]
	 */
	_loadSchema (schemaDir){

		let files = fs.readdirSync(schemaDir);
		files.forEach((file)=>{

			let schemaContent = require(path.join(schemaDir + '/', file));
			let model = schemaContent.title;
			this.schema[model] = schemaContent;
			this[model] = {};
			this[model].create = this.create.bind(this, model);			
			this[model].find = this.find.bind(this, model);
			this[model].delete = this.delete.bind(this, model);	
			this[model].update = this.update.bind(this, model);	
			this.query = this.query.bind(this);
		});
	}

	/**
	 *
	 * eg : 
	 *
	 * schema = {
	 *
	 * 	title : 'user model',
	 * 	type : 'object',
	 * 	properties : {
	 * 		firstName : {
	 * 			type : 'string'
	 * 		},
	 * 		lastName : {
	 * 			type : 'string'
	 * 		},
	 * 		email : {
	 * 			type : 'string',
	 * 			format : 'email'
	 * 		}
	 * 	},
	 * 	required : ['firstName', 'email']
	 * }
	 *
	 * data = {
	 *
	 * 	firstName : 'lahiru',
	 * 	lastName : 'madhumal',
	 * 	email : 'madhumal.lahiru.hd@gmail.com'
	 * 
	 * }
	 * 
	 * _validate(data, schema);
	 * 
	 * validates the data using the json schema given
	 * @param  {Object} data   json object to be validated.
	 * @param  {object} schema json schema object to be validated against.
	 * @return {boolean}        valid or not
	 */
	_validate(data, schema){

		return new Promise((resolve, reject) => {

			let result = tv4.validate(data, schema);

			if(result){
				resolve(result);
			}else{
				reject(tv4.error);
			}			
		}); 
	}

	/**
	 * drop all the tables and create table structure according to the schemas.
	 * @return {[type]} [description]
	 */
	_createTableStructure(){

		this.adapter.createTableStructure(this.schemaDir);

	}

	/**
	 * check differences in the tables and schema and update the structure.
	 * @return {[type]} [description]
	 */
	_alterTableStructure(){
		this.adapter.alterTableStructure(this.schemaDir);
	}

}