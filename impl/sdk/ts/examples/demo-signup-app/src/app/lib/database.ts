import sqlite3, { Database } from 'sqlite3';
import { open } from 'sqlite';
import { Context } from '../../../../../lib/resonate';

async function openDb() {
  return open({
    filename: './mydb.sqlite',
    driver: sqlite3.Database,
  });
}

export async function createAccount(email: string) {
  const db = await openDb();
  await db.run('INSERT OR IGNORE INTO Accounts (email) VALUES (?)', [email]);
}

export async function createCharge(email: string, creditCardNumber: string) {
  const db = await openDb();
  await createChargeSQL(null, db.db, email, creditCardNumber);
}

// create a single durable transaction function
export async function durableTransaction (c: Context, email: string, creditCardNumber: string) {
  durableCreateAccount(c, email);
  durableCreateCharge(c, email, creditCardNumber);
}

export async function durableCreateAccount(c: Context, email: string) {
  const db = await openDb();
  await c.run(createAccountSQL, db.db, email);
}

export async function durableCreateCharge(c: Context, email: string, creditCardNumber: string) {
  const db = await openDb();
  await c.run(createChargeSQL, db.db, email, creditCardNumber);
}

async function createAccountSQL(c: Context, db: Database, email: string) {
  await db.run('INSERT OR IGNORE INTO Accounts (email) VALUES (?)', [email]);
}

async function createChargeSQL(c: Context | null, db: Database, email: string, creditCardNumber: string) {

  // simulate a failure
  // randomly throw an error
  // read the probability from environment variable
  if (Math.random() < 0.9) {
    throw new Error('randomly failed');
  }

  await db.run('INSERT OR IGNORE INTO Charges (email, credit_card_number) VALUES (?, ?)', [email, creditCardNumber]);
}
