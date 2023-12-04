import { open } from 'sqlite';
import sqlite3 from 'sqlite3';

async function setup() {
  const db = await open({
    filename: './mydb.sqlite',
    driver: sqlite3.Database,
  });

  await db.exec(`
    CREATE TABLE IF NOT EXISTS Accounts (
      email TEXT PRIMARY KEY,
      idempotence_key TEXT UNIQUE
    );

    CREATE TABLE IF NOT EXISTS Charges (
      email TEXT,
      credit_card_number TEXT,
      amount INTEGER,
      idempotence_key TEXT UNIQUE
    );
  `);

  console.log('Database setup complete.');
}

setup();
