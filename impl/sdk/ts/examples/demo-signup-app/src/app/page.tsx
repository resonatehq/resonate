"use client"; // This is a client component
import { useState } from 'react';
import styles from './css/Home.module.css'; // Import the CSS file

const Home = () => {
  const [email, setEmail] = useState('');
  const [creditCard, setCreditCard] = useState('');
  const [message, setMessage] = useState('');
  const [messageStatus, setMessageStatus] = useState('false');

  const handleSubmit =  async (e: React.FormEvent) => {
    e.preventDefault();
    const response = await fetch('/api/signup', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email, creditCard }),
    });
    const data = await response.json();

    if (response.status === 200) {
      setMessageStatus('success');
    }

    setMessage(data.message);
  };

  return (
    <div className={styles.container}>
      <img src="/resonate.svg" alt="Resonate"/>
      <h1 className={styles.title}>Dead Simple</h1>
      <br />
      <form className={styles.form} onSubmit={handleSubmit}>
        <input
          type="email"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
          placeholder="Email"
          required
          className={styles.input}
        />
        <input
          type="text"
          value={creditCard}
          onChange={(e) => setCreditCard(e.target.value)}
          placeholder="Credit Card Number"
          required
          className={styles.input}
        />
        <button type="submit" className={styles.button}>
          Submit
        </button>
      </form>
      {message && (
        <p className={messageStatus === 'success' ? styles.message : styles.errorMessage}>
        {message}
      </p>
      )}
    </div>
  );
};

export default Home;
