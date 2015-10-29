import mysql from 'mysql';

/**
 * converts the json object queries for Mysql
 * queries and execte the converted mysql statements.
 */
export default class MySQL {

	constructor(configs) {

		let limit = configs.connectionLimit ? configs.connectionLimit : 10; 
		this.pool = mysql.createPool({
			connectionLimit: limit,
			host: configs.host,
			user: configs.user,
			password: configs.password,
			database: configs.db
		});
	}

	/**
	 * disconnet all the pool connections
	 * @return
	 */
	end() {
		console.log('connection ends');
		this.pool.end();
	}

	/**
	 * find a record from the database.
	 * @param  {Object} options filter object
	 * @return {[type]}       [description]
	 */
	find(table, options) {
		
		let sql = this._findQuery(table, options);

		return new Promise((resolve, reject) => {
			this.pool.query(sql, (error, results, fields) => {
				error ? reject(error) : resolve(results);
			});
		});
	}

	/**
	 * update records in a table
	 * @param  {String} table     table name
	 * @param  {Object} updateObj update values
	 * @param  {Object} opts      update criteria
	 * @return {Object}           result
	 */
	update(table, updateObj, opts) {

		let query = this._updateQuery(table, updateObj, opts);		
		console.log(query);

		return new Promise((resolve, reject) => {
			this.pool.query(query, (error, results, fields) => {
				error ? reject(error) : resolve(results);
			});
		});
	}

	/**
	 * insert a record to the database.
	 * @param  {String} name of the to be inserted
	 * @param  {Object} model object to be inserted.
	 * @return {[type]}       [description]
	 */
	create (table, obj){

		let query = this._insertQuery(table, obj);

		return new Promise((resolve, reject) =>{
			this.pool.query(query, (error, results, fields) =>{
				error ? reject(error) : resolve(results);
			});
		});
	}

	/**
	 * delete records from a table with given constraints.
	 * @param  {string} table
	 * @param  {Object} opts  constraints to be set
	 * @return {Promise}       
	 */
	delete(table, opts){

		let query = this._deleteQuery(table, opts);
		return new Promise((resolve, reject) =>{
			this.pool.query(query, (error, results, fields) => {
				error ? reject(error) : resolve(results);
			});
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
	query(query, values){
		return new Promise((resolve, reject) =>{
			this.pool.query(query, values, (error, results) => {
				error ? reject(error) : resolve(results);
			});
		});
		
	}

	/**
	 * [createTableStructure description]
	 * @param  {[type]} schemaDir [description]
	 * @return {[type]}           [description]
	 */
	createTableStructure(schemaDir) {

	}

	/**
	 * [alterTableStructure description]
	 * @param  {[type]} schemaDir [description]
	 * @return {[type]}           [description]
	 */
	alterTableStructure(schemaDir) {

	}


	/**
	 * generate the query string for insert.
	 * 
	 * insert a record to the database.
	 * @param  {String} name of the to be inserted
	 * @param  {Object} model object to be inserted.
	 * @return {String} returns the query string.
	 */
	_insertQuery (table, data){

		let fields = Object.keys(data).join();
		let valuesArr = Object.values(data);
		let values = "";
		valuesArr.forEach((value) => {
			values += "'" + value +"',";
		});

		values = values.substr(0, values.length-1);

		let query = "INSERT INTO " + table + " ("+ fields + ") VALUES (" + values +");";

		return query;
	}

	/**
	 * Generates the query string for select query
	 * @param  {String} table table name
	 * @param  {Object} opts  select criteria
	 * @return {String}       [query string]
	 */
	_findQuery(table, opts) {

		let query = "";
		let selectColumns = opts.selectColumns; //select Columns type sould be an array 

		if(!selectColumns){
			query += "SELECT * FROM " + table + " WHERE " + this._queryBuilder(opts);
		}else {
			let columns = "";
			query += "SELECT ";
			selectColumns.forEach((column) =>{
				columns += column +",";
			});

			columns = columns.substr(0,columns.length-1);
			delete opts.selectColumns;
			query += columns + " FROM " + table + " WHERE " + this._queryBuilder(opts);
		}
		return query;
	}

	/**
	 * Generates the query string for update query
	 * @param  {String} table     table to be updated
	 * @param  {Object} updateObj update values
	 * @param  {Object} opts      update criteria
	 * @return {String}           returns the update query string
	 */
	_updateQuery(table, updateObj, opts) {

		let query = "";
		let updateKeys = Object.keys(updateObj);

		if(!updateObj || updateKeys.length == 0){
			throw Error('update tables or values are not set.');
			return null;
		}

		query += "UPDATE " + table + " SET";
		let setString = "";
		updateKeys.forEach((key) =>{
			setString += " " + key + "='" + updateObj[key] + "',";
		});
		setString = setString.substr(0,setString.length-1);

		query += setString + " WHERE " + this._queryBuilder(opts);
		return query;
	}

	/**
	 * generates the query string for a delete operation
	 * @param  {string} table table name of the record to be deleted.
	 * @param  {Object} opts  restrictions as a json object.
	 * @return {String}       delete query string.
	 */
	_deleteQuery(table, opts) {
		let query = "DELETE FROM " + table + " WHERE " +this._queryBuilder(opts);
		return query;
	}

	_queryBuilder(opts = {}){

		let query = "";

		Object.keys(opts).map((key) =>{

			if(key == 'or'){
				query += " AND " + this._orQuery(opts[key]);
			}else if (typeof opts[key] != 'object'){
				query += " AND " + key + "='" + opts[key] +"'";
			}else if (typeof opts[key] == 'object' && Object.keys(opts[key]).length == 1) {
				Object.keys(opts[key]).map((idx) =>{
					if(idx == '<' || idx == '>')
						query += " AND " + this._compareQuery(key, opts[key]) + " ";
				});
			}
		});

		query = query.substr(4,query.length-1);
		return query;
	}

	/**
	 * ceates a query of the type (exp || exp || exp) //eg for exp : age='28' & name = "gayan"
	 * @param  {Ojbect} orArr query obect
	 * @return {String}
	 */
	_orQuery (orArr){

		let orQ = "(";
		orArr.forEach((obj) =>{

			let keys = Object.keys(obj);

			if(keys.length > 1){
				orQ += this._andQuery(obj) + " OR ";
			}else if(keys.length == 1 && typeof obj[keys[0]] != 'object'){
				orQ += keys[0] + "='" + obj[keys[0]] + "' OR ";
			}else if(keys.length == 1 && typeof obj[keys[0]] == 'object'){
				let orObj = obj[keys[0]];
				let orObjKeys = Object.keys(orObj);
				if(orObjKeys.length == 1 && (orObjKeys[0] == '<' || orObjKeys[0] == '>'))
					orQ += this._compareQuery(keys[0], orObj) + " OR ";
			}
		});

		orQ = orQ.substr(0, orQ.length-3);
		orQ +=")";
		return orQ;
	}

	/**
	 * creates a query of the type (exp and exp and exp) // eg for exp : name='madhumal' || age = '28'
	 * @param  {Object} obj query object
	 * @return {String}
	 */
	_andQuery (obj){

		let andQuery = "(";
		Object.keys(obj).map((key) =>{

			if(key == 'or'){
				andQuery += this._orQuery(obj[key]) + " AND ";
			}else if(typeof obj[key] != 'object'){
				andQuery += key + "='" + obj[key] + "' AND ";
			}else if(typeof obj[key] == 'object' && Object.keys(obj[key]).length == 1){
				Object.keys(obj[key]).map((idx) =>{
					if(idx == '<' || idx == '>')
						andQuery += this._compareQuery(key, obj[key]) + " AND ";
				});
			}
		});
		andQuery = andQuery.substr(0,andQuery.length-4);
		andQuery += ")";
		return andQuery;
	}

	/**
	 * returns a string similar to variable<value , variable>value
	 * @param  {string} objKey
	 * @param  {object} obj   query object
	 * @return {string}
	 */
	_compareQuery(objKey, obj){

		let compQuery = "";
		let keys = Object.keys(obj);

		if(keys.length != 1){
			throw Error('Format error : compare object cannot have multiple keys');
		} else{
			keys.map((key) =>{

				if(typeof obj[key] == 'object'){
					throw Error ('Format error : compare value cannot be an object');
				}else{
					compQuery = objKey + " " + key + "'" + obj[key] + "'";
				}
			});
		}

		return compQuery;
	}	
}