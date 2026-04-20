import React from 'react';
import { useOutletContext } from 'react-router-dom';
import Overview from './pages/Overview';
import Transactions from './pages/Transactions';
import Accounts from './pages/Accounts';
import Analytics from './pages/Analytics';
import '../styles/Dashboard.css';

const Dashboard = () => {
  const { activePage } = useOutletContext() || { activePage: 'dashboard' };

  return (
    <React.Fragment>
      {activePage === 'dashboard' && <Overview />}
      {activePage === 'transactions' && <Transactions />}
      {activePage === 'accounts' && <Accounts />}
      {activePage === 'analytics' && <Analytics />}
    </React.Fragment>
  );
};

export default Dashboard;
