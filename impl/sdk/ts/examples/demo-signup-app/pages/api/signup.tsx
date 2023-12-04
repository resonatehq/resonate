import type { NextApiRequest, NextApiResponse } from 'next';
import { createAccount, createCharge, durableTransaction} from '../../src/app/lib/database';
import { Resonate } from '../../../../lib/resonate';

if (process.env.RESONATE === 'true') {
  console.log('Using resonate');
} else {
  console.log('Using functions directly');
}

const resonate = new Resonate();
resonate.register('resonateTransaction', durableTransaction);

const signupHandler = async (req: NextApiRequest, res: NextApiResponse) => {
  if (req.method === 'POST') {
    const { email, creditCard } = req.body;

    if (!email || !creditCard) {
      res.status(400).json({ message: 'Missing email or credit card' });
      return;
    }

    // check if resonate env var is set to true
    // if so, use resonate to run the functions
    // otherwise, use the functions directly

    if (process.env.RESONATE === 'true') {
      await resonate.run('resonateTransaction', email, creditCard);
      res.status(200).json({ message: 'Transaction Successful. Registration Completed.' });
      return;
    }
    await createAccount(email);
    try {
      await createCharge(email, creditCard);
      res.status(200).json({ message: 'Transaction Successful. Registration Completed.' });
    } catch (e) {
      console.error(e);
      res.status(500).json({ message: 'Error while charging credit card. Lost transaction.' });
    }
  } else {
    res.status(405).json({ message: 'Method not allowed' });
  }
};

export default signupHandler;

