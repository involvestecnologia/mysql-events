/* eslint-disable padded-blocks,no-unused-expressions,no-await-in-loop */

const chai = require('chai');
const mysql = require('mysql');
const MySQLEvents = require('./lib');

const { expect } = chai;

const TEST_SCHEMA_1 = 'testSchema1';
const TEST_SCHEMA_2 = 'testSchema2';
const TEST_TABLE_1 = 'testTable1';
const TEST_TABLE_2 = 'testTable2';
const TEST_COLUMN_1 = 'column1';
const TEST_COLUMN_2 = 'column2';

const delay = (timeout = 500) => new Promise((resolve) => {
  setTimeout(resolve, timeout);
});

const getConnection = () => {
  const connection = mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: 'root',
  });

  return new Promise((resolve, reject) => connection.connect((err) => {
    if (err) return reject(err);
    resolve(connection);
  }));
};

const executeQuery = (conn, query) => {
  return new Promise((resolve, reject) => conn.query(query, (err, results) => {
    if (err) return reject(err);
    resolve(results);
  }));
};

const closeConnection = conn => new Promise((resolve, reject) => conn.end((err) => {
  if (err) return reject(err);
  resolve();
}));

const grantPrivileges = async () => {
  const conn = await getConnection();
  try {
    await executeQuery(conn, 'GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO \'root\'@\'localhost\'');
  } catch (err) {
    throw err;
  } finally {
    await closeConnection(conn);
  }
};

const createSchemas = async () => {
  const conn = await getConnection();
  try {
    await executeQuery(conn, `CREATE DATABASE IF NOT EXISTS ${TEST_SCHEMA_1};`);
    await executeQuery(conn, `CREATE DATABASE IF NOT EXISTS ${TEST_SCHEMA_2};`);
  } catch (err) {
    throw err;
  } finally {
    await closeConnection(conn);
  }
};

const dropSchemas = async () => {
  const conn = await getConnection();
  try {
    await executeQuery(conn, `DROP DATABASE IF EXISTS ${TEST_SCHEMA_1};`);
    await executeQuery(conn, `DROP DATABASE IF EXISTS ${TEST_SCHEMA_2};`);
  } catch (err) {
    throw err;
  } finally {
    await closeConnection(conn);
  }
};

const createTables = async () => {
  const conn = await getConnection();
  try {
    await executeQuery(conn, `USE ${TEST_SCHEMA_1};`);
    await executeQuery(conn, `CREATE TABLE IF NOT EXISTS ${TEST_TABLE_1} (${TEST_COLUMN_1} varchar(255), ${TEST_COLUMN_2} varchar(255));`);
    await executeQuery(conn, `CREATE TABLE IF NOT EXISTS ${TEST_TABLE_2} (${TEST_COLUMN_1} varchar(255), ${TEST_COLUMN_2} varchar(255));`);
    await executeQuery(conn, `USE ${TEST_SCHEMA_2};`);
    await executeQuery(conn, `CREATE TABLE IF NOT EXISTS ${TEST_TABLE_1} (${TEST_COLUMN_1} varchar(255), ${TEST_COLUMN_2} varchar(255));`);
    await executeQuery(conn, `CREATE TABLE IF NOT EXISTS ${TEST_TABLE_2} (${TEST_COLUMN_1} varchar(255), ${TEST_COLUMN_2} varchar(255));`);
  } catch (err) {
    throw err;
  } finally {
    await closeConnection(conn);
  }
};

const dropTables = async () => {
  const conn = await getConnection();
  try {
    await executeQuery(conn, `USE ${TEST_SCHEMA_1};`);
    await executeQuery(conn, `DROP TABLE IF EXISTS ${TEST_TABLE_1};`);
    await executeQuery(conn, `DROP TABLE IF EXISTS ${TEST_TABLE_2};`);
  } catch (err) {
    throw err;
  } finally {
    await closeConnection(conn);
  }
};

before(async () => {
  chai.should();
  await createSchemas();
  await grantPrivileges();
});

beforeEach(async () => {
  await createTables();
});

afterEach(async () => {
  await dropTables();
});

after(async () => {
  await dropSchemas();
});

describe('MySQLEvents', () => {

  it('should expose EVENTS enum', async () => {
    MySQLEvents.EVENTS.should.be.an('object');
    MySQLEvents.EVENTS.should.have.ownPropertyDescriptor('BINLOG');
    MySQLEvents.EVENTS.BINLOG.should.be.equal('binlog');
    MySQLEvents.EVENTS.should.have.ownPropertyDescriptor('TRIGGER_ERROR');
    MySQLEvents.EVENTS.TRIGGER_ERROR.should.be.equal('triggerError');
    MySQLEvents.EVENTS.should.have.ownPropertyDescriptor('CONNECTION_ERROR');
    MySQLEvents.EVENTS.CONNECTION_ERROR.should.be.equal('connectionError');
    MySQLEvents.EVENTS.should.have.ownPropertyDescriptor('ZONGJI_ERROR');
    MySQLEvents.EVENTS.ZONGJI_ERROR.should.be.equal('zongjiError');
  });

  it('should expose STATEMENTS enum', async () => {
    MySQLEvents.STATEMENTS.should.be.an('object');
    MySQLEvents.STATEMENTS.should.have.ownPropertyDescriptor('ALL');
    MySQLEvents.STATEMENTS.ALL.should.be.equal('ALL');
    MySQLEvents.STATEMENTS.should.have.ownPropertyDescriptor('INSERT');
    MySQLEvents.STATEMENTS.INSERT.should.be.equal('INSERT');
    MySQLEvents.STATEMENTS.should.have.ownPropertyDescriptor('UPDATE');
    MySQLEvents.STATEMENTS.UPDATE.should.be.equal('UPDATE');
    MySQLEvents.STATEMENTS.should.have.ownPropertyDescriptor('DELETE');
    MySQLEvents.STATEMENTS.DELETE.should.be.equal('DELETE');
  });

  it('should connect and disconnect from MySQL using a pre existing connection', async () => {
    const connection = mysql.createConnection({
      host: 'localhost',
      user: 'root',
      password: 'root',
    });

    const instance = new MySQLEvents(connection);

    await instance.start();

    await delay();

    await instance.stop();
  }).timeout(10000);

  it('should connect and disconnect from MySQL using a dsn', async () => {
    const instance = new MySQLEvents({
      host: 'localhost',
      user: 'root',
      password: 'root',
    });

    await instance.start();

    await delay();

    await instance.stop();
  }).timeout(10000);

  it('should connect and disconnect from MySQL using a connection string', async () => {
    const instance = new MySQLEvents(`mysql://root:root@localhost/${TEST_SCHEMA_1}`);

    await instance.start();

    await delay();

    await instance.stop();
  }).timeout(10000);

  it('should catch an event through an INSERT trigger', async () => {
    const instance = new MySQLEvents({
      host: 'localhost',
      user: 'root',
      password: 'root',
    }, {
      startAtEnd: true,
      excludedSchemas: {
        mysql: true,
      },
    });

    await instance.start();

    let triggerEvent = null;
    instance.addTrigger({
      name: 'Test',
      expression: `${TEST_SCHEMA_1}.${TEST_TABLE_1}`,
      statement: MySQLEvents.STATEMENTS.INSERT,
      onEvent: (event) => {
        triggerEvent = event;
      },
    });

    instance.on(MySQLEvents.EVENTS.TRIGGER_ERROR, console.error)
    instance.on(MySQLEvents.EVENTS.CONNECTION_ERROR, console.error)
    instance.on(MySQLEvents.EVENTS.ZONGJI_ERROR, console.error)

    await executeQuery(instance.connection, `USE ${TEST_SCHEMA_1};`);
    await executeQuery(instance.connection, `INSERT INTO ${TEST_TABLE_1} VALUES ('test1', 'test2');`);

    await delay(1000);

    if (!triggerEvent) throw new Error('No trigger was caught');

    triggerEvent.should.be.an('object');

    triggerEvent.should.have.ownPropertyDescriptor('type');
    triggerEvent.type.should.be.a('string').equals('INSERT');

    triggerEvent.should.have.ownPropertyDescriptor('timestamp');
    triggerEvent.timestamp.should.be.a('number');

    triggerEvent.should.have.ownPropertyDescriptor('table');
    triggerEvent.table.should.be.a('string').equals(TEST_TABLE_1);

    triggerEvent.should.have.ownPropertyDescriptor('schema');
    triggerEvent.schema.should.be.a('string').equals(TEST_SCHEMA_1);

    triggerEvent.should.have.ownPropertyDescriptor('nextPosition');
    triggerEvent.nextPosition.should.be.a('number');

    triggerEvent.should.have.ownPropertyDescriptor('affectedRows');
    triggerEvent.affectedRows.should.be.an('array').to.have.lengthOf(1);
    triggerEvent.affectedRows[0].should.be.an('object');
    triggerEvent.affectedRows[0].should.have.ownPropertyDescriptor('after');
    triggerEvent.affectedRows[0].after.should.be.an('object');
    triggerEvent.affectedRows[0].after.should.have.ownPropertyDescriptor(TEST_COLUMN_1);
    triggerEvent.affectedRows[0].after[TEST_COLUMN_1].should.be.a('string').equals('test1');
    triggerEvent.affectedRows[0].after.should.have.ownPropertyDescriptor(TEST_COLUMN_2);
    triggerEvent.affectedRows[0].after[TEST_COLUMN_2].should.be.a('string').equals('test2');
    triggerEvent.affectedRows[0].should.have.ownPropertyDescriptor('before');
    expect(triggerEvent.affectedRows[0].before).to.be.an('undefined');

    await instance.stop();
  }).timeout(10000);

  it('should catch an event through an UPDATE trigger', async () => {
    const instance = new MySQLEvents({
      host: 'localhost',
      user: 'root',
      password: 'root',
    }, {
      startAtEnd: true,
      excludedSchemas: {
        mysql: true,
      },
    });

    await instance.start();

    let triggerEvent = null;
    instance.addTrigger({
      name: 'Test',
      expression: `${TEST_SCHEMA_1}.${TEST_TABLE_1}`,
      statement: MySQLEvents.STATEMENTS.UPDATE,
      onEvent: (event) => {
        triggerEvent = event;
      },
    });

    await executeQuery(instance.connection, `USE ${TEST_SCHEMA_1};`);
    await executeQuery(instance.connection, `INSERT INTO ${TEST_TABLE_1} VALUES ('test1', 'test2');`);
    await executeQuery(instance.connection, `UPDATE ${TEST_TABLE_1} SET ${TEST_COLUMN_1} = 'test3', ${TEST_COLUMN_2} = 'test4';`);

    await delay(1000);

    if (!triggerEvent) throw new Error('No trigger was caught');

    triggerEvent.should.be.an('object');

    triggerEvent.should.have.ownPropertyDescriptor('type');
    triggerEvent.type.should.be.a('string').equals('UPDATE');

    triggerEvent.should.have.ownPropertyDescriptor('timestamp');
    triggerEvent.timestamp.should.be.a('number');

    triggerEvent.should.have.ownPropertyDescriptor('table');
    triggerEvent.table.should.be.a('string').equals(TEST_TABLE_1);

    triggerEvent.should.have.ownPropertyDescriptor('schema');
    triggerEvent.schema.should.be.a('string').equals(TEST_SCHEMA_1);

    triggerEvent.should.have.ownPropertyDescriptor('nextPosition');
    triggerEvent.nextPosition.should.be.a('number');

    triggerEvent.should.have.ownPropertyDescriptor('affectedRows');
    triggerEvent.affectedRows.should.be.an('array').to.have.lengthOf(1);
    triggerEvent.affectedRows[0].should.be.an('object');

    triggerEvent.affectedRows[0].should.have.ownPropertyDescriptor('after');
    triggerEvent.affectedRows[0].after.should.be.an('object');
    triggerEvent.affectedRows[0].after.should.have.ownPropertyDescriptor(TEST_COLUMN_1);
    triggerEvent.affectedRows[0].after[TEST_COLUMN_1].should.be.a('string').equals('test3');
    triggerEvent.affectedRows[0].after.should.have.ownPropertyDescriptor(TEST_COLUMN_2);
    triggerEvent.affectedRows[0].after[TEST_COLUMN_2].should.be.a('string').equals('test4');

    triggerEvent.affectedRows[0].should.have.ownPropertyDescriptor('before');
    triggerEvent.affectedRows[0].before.should.be.an('object');
    triggerEvent.affectedRows[0].before.should.have.ownPropertyDescriptor(TEST_COLUMN_1);
    triggerEvent.affectedRows[0].before[TEST_COLUMN_1].should.be.a('string').equals('test1');
    triggerEvent.affectedRows[0].before.should.have.ownPropertyDescriptor(TEST_COLUMN_2);
    triggerEvent.affectedRows[0].before[TEST_COLUMN_2].should.be.a('string').equals('test2');

    await instance.stop();
  }).timeout(10000);

  it('should catch an event through an DELETE trigger', async () => {
    const instance = new MySQLEvents({
      host: 'localhost',
      user: 'root',
      password: 'root',
    }, {
      startAtEnd: true,
      excludedSchemas: {
        mysql: true,
      },
    });

    await instance.start();

    let triggerEvent = null;
    instance.addTrigger({
      name: 'Test',
      expression: `${TEST_SCHEMA_1}.${TEST_TABLE_1}`,
      statement: MySQLEvents.STATEMENTS.DELETE,
      onEvent: (event) => {
        triggerEvent = event;
      },
    });

    await executeQuery(instance.connection, `USE ${TEST_SCHEMA_1};`);
    await executeQuery(instance.connection, `INSERT INTO ${TEST_TABLE_1} VALUES ('test1', 'test2');`);
    await executeQuery(instance.connection, `DELETE FROM ${TEST_TABLE_1} WHERE ${TEST_COLUMN_1} = 'test1' AND ${TEST_COLUMN_2} = 'test2';`);

    await delay(1000);

    if (!triggerEvent) throw new Error('No trigger was caught');

    triggerEvent.should.be.an('object');

    triggerEvent.should.have.ownPropertyDescriptor('type');
    triggerEvent.type.should.be.a('string').equals('DELETE');

    triggerEvent.should.have.ownPropertyDescriptor('timestamp');
    triggerEvent.timestamp.should.be.a('number');

    triggerEvent.should.have.ownPropertyDescriptor('table');
    triggerEvent.table.should.be.a('string').equals(TEST_TABLE_1);

    triggerEvent.should.have.ownPropertyDescriptor('schema');
    triggerEvent.schema.should.be.a('string').equals(TEST_SCHEMA_1);

    triggerEvent.should.have.ownPropertyDescriptor('nextPosition');
    triggerEvent.nextPosition.should.be.a('number');

    triggerEvent.should.have.ownPropertyDescriptor('affectedRows');
    triggerEvent.affectedRows.should.be.an('array').to.have.lengthOf(1);
    triggerEvent.affectedRows[0].should.be.an('object');

    triggerEvent.affectedRows[0].should.have.ownPropertyDescriptor('after');
    expect(triggerEvent.affectedRows[0].after).to.be.an('undefined');

    triggerEvent.affectedRows[0].should.have.ownPropertyDescriptor('before');
    triggerEvent.affectedRows[0].before.should.be.an('object');
    triggerEvent.affectedRows[0].before.should.have.ownPropertyDescriptor(TEST_COLUMN_1);
    triggerEvent.affectedRows[0].before[TEST_COLUMN_1].should.be.a('string').equals('test1');
    triggerEvent.affectedRows[0].before.should.have.ownPropertyDescriptor(TEST_COLUMN_2);
    triggerEvent.affectedRows[0].before[TEST_COLUMN_2].should.be.a('string').equals('test2');

    await instance.stop();
  }).timeout(10000);

  it('should catch events through an ALL trigger', async () => {
    const instance = new MySQLEvents({
      host: 'localhost',
      user: 'root',
      password: 'root',
    }, {
      startAtEnd: true,
      excludedSchemas: {
        mysql: true,
      },
    });

    await instance.start();

    const triggerEvents = [];
    instance.addTrigger({
      name: 'Test',
      expression: `${TEST_SCHEMA_1}.${TEST_TABLE_1}`,
      statement: MySQLEvents.STATEMENTS.ALL,
      onEvent: (event) => {
        triggerEvents.push(event);
      },
    });

    await executeQuery(instance.connection, `USE ${TEST_SCHEMA_1};`);
    await executeQuery(instance.connection, `INSERT INTO ${TEST_TABLE_1} VALUES ('test1', 'test2');`);
    await executeQuery(instance.connection, `UPDATE ${TEST_TABLE_1} SET ${TEST_COLUMN_1} = 'test3', ${TEST_COLUMN_2} = 'test4';`);
    await executeQuery(instance.connection, `DELETE FROM ${TEST_TABLE_1} WHERE ${TEST_COLUMN_1} = 'test3' AND ${TEST_COLUMN_2} = 'test4';`);

    await delay(1000);

    expect(triggerEvents).to.be.an('array').that.is.not.empty;

    triggerEvents[0].should.have.ownPropertyDescriptor('type');
    triggerEvents[0].type.should.be.a('string').equals('INSERT');

    triggerEvents[1].should.have.ownPropertyDescriptor('type');
    triggerEvents[1].type.should.be.a('string').equals('UPDATE');

    triggerEvents[2].should.have.ownPropertyDescriptor('type');
    triggerEvents[2].type.should.be.a('string').equals('DELETE');

    await instance.stop();
  }).timeout(10000);

  it('should remove a previously added event trigger', async () => {
    const instance = new MySQLEvents({
      host: 'localhost',
      user: 'root',
      password: 'root',
    });

    instance.addTrigger({
      name: 'Test',
      expression: `${TEST_SCHEMA_1}.${TEST_TABLE_1}`,
      statement: MySQLEvents.STATEMENTS.ALL,
      onEvent: () => {},
    });

    instance.expressions[`${TEST_SCHEMA_1}.${TEST_TABLE_1}`].statements[MySQLEvents.STATEMENTS.ALL].should.be.an('array').that.is.not.empty;

    instance.expressions[`${TEST_SCHEMA_1}.${TEST_TABLE_1}`].statements[MySQLEvents.STATEMENTS.ALL][0].should.be.an('object');
    instance.expressions[`${TEST_SCHEMA_1}.${TEST_TABLE_1}`].statements[MySQLEvents.STATEMENTS.ALL][0].name.should.be.a('string').equals('Test');
    instance.expressions[`${TEST_SCHEMA_1}.${TEST_TABLE_1}`].statements[MySQLEvents.STATEMENTS.ALL][0].onEvent.should.be.a('function');

    instance.removeTrigger({
      name: 'Test',
      expression: `${TEST_SCHEMA_1}.${TEST_TABLE_1}`,
      statement: MySQLEvents.STATEMENTS.ALL,
    });

    expect(instance.expressions[`${TEST_SCHEMA_1}.${TEST_TABLE_1}`].statements[MySQLEvents.STATEMENTS.ALL][0]).to.be.an('undefined');

    await instance.stop();
  }).timeout(10000);

  it('should throw an error when adding duplicated trigger name for a statement', async () => {
    const instance = new MySQLEvents({
      host: 'localhost',
      user: 'root',
      password: 'root',
    });

    instance.addTrigger({
      name: 'Test',
      expression: `${TEST_SCHEMA_1}.${TEST_TABLE_1}`,
      statement: MySQLEvents.STATEMENTS.ALL,
      onEvent: () => {},
    });

    expect(() => instance.addTrigger({
      name: 'Test',
      expression: `${TEST_SCHEMA_1}.${TEST_TABLE_1}`,
      statement: MySQLEvents.STATEMENTS.ALL,
      onEvent: () => {},
    })).to.throw(Error);
  });

  it('should emit an event when a trigger produces an error', async () => {
    const instance = new MySQLEvents({
      host: 'localhost',
      user: 'root',
      password: 'root',
    }, {
      startAtEnd: true,
      excludedSchemas: {
        mysql: true,
      },
    });

    await instance.start();

    await delay();

    let error = null;
    instance.on(MySQLEvents.EVENTS.TRIGGER_ERROR, (err) => {
      error = err;
    });

    instance.addTrigger({
      name: 'Test',
      expression: `${TEST_SCHEMA_1}.${TEST_TABLE_1}`,
      statement: MySQLEvents.STATEMENTS.ALL,
      onEvent: () => {
        throw new Error('Error');
      },
    });

    await executeQuery(instance.connection, `USE ${TEST_SCHEMA_1};`);
    await executeQuery(instance.connection, `INSERT INTO ${TEST_TABLE_1} VALUES ('test1', 'test2');`);

    await delay();

    expect(error).to.be.an('object');
    error.trigger.should.be.an('object');
    error.error.should.be.an('Error');
  });

  it('should receive events from multiple schemas', async () => {
    const instance = new MySQLEvents({
      host: 'localhost',
      user: 'root',
      password: 'root',
    }, {
      startAtEnd: true,
      excludedSchemas: {
        mysql: true,
      },
    });

    await instance.start();

    const triggeredEvents = [];
    instance.addTrigger({
      name: 'Test',
      expression: `${TEST_SCHEMA_1}`,
      statement: MySQLEvents.STATEMENTS.UPDATE,
      onEvent: (event) => {
        triggeredEvents.push(event);
      },
    });
    instance.addTrigger({
      name: 'Test2',
      expression: `${TEST_SCHEMA_2}`,
      statement: MySQLEvents.STATEMENTS.ALL,
      onEvent: (event) => {
        triggeredEvents.push(event);
      },
    });

    await executeQuery(instance.connection, `USE ${TEST_SCHEMA_1};`);
    await executeQuery(instance.connection, `INSERT INTO ${TEST_TABLE_1} VALUES ('test1', 'test2');`);
    await executeQuery(instance.connection, `UPDATE ${TEST_TABLE_1} SET ${TEST_COLUMN_1} = 'test3', ${TEST_COLUMN_2} = 'test4';`);

    await executeQuery(instance.connection, `USE ${TEST_SCHEMA_2};`);
    await executeQuery(instance.connection, `INSERT INTO ${TEST_TABLE_1} VALUES ('test1', 'test2');`);
    await executeQuery(instance.connection, `UPDATE ${TEST_TABLE_1} SET ${TEST_COLUMN_1} = 'test3', ${TEST_COLUMN_2} = 'test4';`);

    await delay(1000);

    if (!triggeredEvents.length) throw new Error('No trigger was caught');
  }).timeout(15000);

  it('should pause and resume connection', async () => {
    const connection = mysql.createConnection({
      host: 'localhost',
      user: 'root',
      password: 'root',
    });

    const instance = new MySQLEvents({
      host: 'localhost',
      user: 'root',
      password: 'root',
    }, {
      startAtEnd: true,
      excludedSchemas: {
        mysql: true,
      },
    });

    await instance.start();

    const triggeredEvents = [];
    instance.addTrigger({
      name: 'Test',
      expression: `${TEST_SCHEMA_1}`,
      statement: MySQLEvents.STATEMENTS.ALL,
      onEvent: (event) => {
        triggeredEvents.push(event);
      },
    });

    await executeQuery(connection, `USE ${TEST_SCHEMA_1};`);
    await executeQuery(connection, `INSERT INTO ${TEST_TABLE_1} VALUES ('test1', 'test2');`);
    await executeQuery(connection, `UPDATE ${TEST_TABLE_1} SET ${TEST_COLUMN_1} = 'test3', ${TEST_COLUMN_2} = 'test4';`);

    await delay(1000);

    if (!triggeredEvents.length) throw new Error('No trigger was caught');
    triggeredEvents.splice(0);

    instance.pause();
    await delay(300);

    await executeQuery(connection, `USE ${TEST_SCHEMA_1};`);
    await executeQuery(connection, `INSERT INTO ${TEST_TABLE_1} VALUES ('test3', 'test4');`);
    await executeQuery(connection, `UPDATE ${TEST_TABLE_1} SET ${TEST_COLUMN_1} = 'test4', ${TEST_COLUMN_2} = 'test5';`);

    await delay(1000);

    if (triggeredEvents.length) throw new Error('Connection should be stopped');

    instance.resume();

    await delay(1000);

    if (!triggeredEvents.length) throw new Error('No trigger was caught');
  }).timeout(15000);

});
