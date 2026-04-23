import React, { useState, useEffect } from 'react';
import { useHeartbeat } from './hooks/useHeartbeat';
import { Routes, Route, Navigate } from 'react-router-dom';
import { useAuth } from './shared/hooks/useAuth';
import { useRole } from './context/RoleContext';
import { supabase, supabaseConfigError } from './shared/supabase';
import { ParsingProvider } from './context/ParsingContext';
//chatbot
import LedgerBuddy from './components/chatbot/LedgerBuddy';
// Pages & Components
import AuthPage from './components/AuthPage';
import Overview from './components/pages/Overview';
import Transactions from './components/pages/Transactions';
import Accounts from './components/pages/Accounts';
import Analytics from './components/pages/Analytics';
import WelcomeScreen from './components/WelcomeScreen';
import SetupAccounts from './components/SetupAccounts';
import QCPanel from './components/QCPanel';

// Parser Module Components
import ParsingPage from './pages/Parsing';
import ReviewPage from './pages/Review';

// Layouts & Protection
import AuthLayout from './layouts/AuthLayout';
import AppLayout from './layouts/AppLayout';
import QCLayout from './layouts/QCLayout';
import ProtectedRoute from './components/ProtectedRoute';

// Guard component to handle setup check redirects without breaking route matching
const ModuleGuard = ({ hasModules, hasIdentifiers, checkSetupStatus, user, toggleTheme, isDarkMode }) => {
  if (hasModules === false) {
    return <WelcomeScreen onSetupComplete={checkSetupStatus} toggleTheme={toggleTheme} isDarkMode={isDarkMode} />;
  }
  if (hasIdentifiers === false) {
    return <SetupAccounts onSetupAccountsComplete={checkSetupStatus} />;
  }
  return <AppLayout user={user} toggleTheme={toggleTheme} isDarkMode={isDarkMode} />;
};

function App() {
  const { user, loading: authLoading } = useAuth();
  // Keep Render free-tier services awake while the user is active.
  useHeartbeat();
  const { role, roleLoading } = useRole();
  const [isDarkMode, setIsDarkMode] = useState(false);
  const [hasModules, setHasModules] = useState(null);
  const [hasIdentifiers, setHasIdentifiers] = useState(null);
  const [loading, setLoading] = useState(true);

  const toggleTheme = () => setIsDarkMode(!isDarkMode);

  useEffect(() => {
    if (isDarkMode) {
      document.body.classList.remove('light-mode');
    } else {
      document.body.classList.add('light-mode');
    }
  }, [isDarkMode]);

  const checkSetupStatus = async () => {
    if (!supabase) {
      setHasModules(false);
      setHasIdentifiers(false);
      setLoading(false);
      return;
    }

    if (!user) {
      setHasModules(null);
      setHasIdentifiers(null);
      setLoading(false);
      return;
    }

    try {
      if (role === 'QC' || role === 'ADMIN') {
         setLoading(false);
         return;
      }

      // Check Modules
      const { data: modules, error: modErr } = await supabase
        .from('user_modules')
        .select('module_id')
        .eq('user_id', user.id);

      if (modErr) throw modErr;
      const modulesExist = modules && modules.length > 0;
      setHasModules(modulesExist);

      if (modulesExist) {
        const { data: identifiers, error: idErr } = await supabase
          .from('account_identifiers')
          .select('identifier_id')
          .eq('user_id', user.id)
          .not('account_number_last4', 'is', null);

        if (idErr) throw idErr;
        setHasIdentifiers(identifiers && identifiers.length > 0);
      } else {
        setHasIdentifiers(false);
      }
    } catch (err) {
      console.error('Error checking setup status:', err);
      setHasModules(false);
      setHasIdentifiers(false);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (user && role) checkSetupStatus();
    else if (!user) setLoading(false);
  }, [user, role]);

  if (supabaseConfigError) {
    return (
      <div style={{ minHeight: '100vh', display: 'grid', placeItems: 'center', backgroundColor: '#0f172a', color: '#e2e8f0', fontFamily: 'Inter, sans-serif', padding: '24px' }}>
        <div style={{ maxWidth: '760px', width: '100%', lineHeight: 1.6 }}>
          <h1 style={{ margin: '0 0 8px 0', fontSize: '1.5rem' }}>Configuration Required</h1>
          <p style={{ margin: '0 0 12px 0', color: '#cbd5e1' }}>{supabaseConfigError}</p>
          <p style={{ margin: 0, color: '#94a3b8' }}>Update frontend-web/.env, then restart the Vite dev server.</p>
        </div>
      </div>
    );
  }

  // Wait for Auth AND Role to minimize transient "no-user" states during refresh
  if (authLoading || roleLoading || (user && loading)) {
    return (
      <div style={{ height: '100vh', display: 'grid', placeItems: 'center', backgroundColor: 'var(--bg-primary, #0f172a)', color: 'var(--text-primary, #e2e8f0)', fontFamily: 'Inter, sans-serif' }}>
        Initializing LedgerAI...
      </div>
    );
  }

  return (
    <ParsingProvider>
      <Routes>
        <Route path="/auth" element={user && (window.location.pathname.startsWith('/auth')) ? <Navigate to={role === 'QC' ? "/qc" : "/"} replace /> : <AuthLayout />}>
           <Route index element={<AuthPage toggleTheme={toggleTheme} isDarkMode={isDarkMode} />} />
           <Route path="login" element={<AuthPage toggleTheme={toggleTheme} isDarkMode={isDarkMode} />} />
        </Route>

        <Route path="/qc" element={
            <ProtectedRoute allowedRoles={['QC', 'ADMIN']}>
               <QCLayout user={user} toggleTheme={toggleTheme} isDarkMode={isDarkMode} />
            </ProtectedRoute>
        }>
           <Route index element={<QCPanel user={user} toggleTheme={toggleTheme} isDarkMode={isDarkMode} />} />
        </Route>

        <Route path="/" element={
            <ProtectedRoute allowedRoles={['USER', 'ADMIN']}>
               <ModuleGuard 
                  hasModules={hasModules} 
                  hasIdentifiers={hasIdentifiers} 
                  checkSetupStatus={checkSetupStatus} 
                  user={user} 
                  toggleTheme={toggleTheme} 
                  isDarkMode={isDarkMode} 
               />
            </ProtectedRoute>
        }>
             <Route index element={<Overview />} />
             <Route path="overview" element={<Overview />} />
             <Route path="parsing" element={<ParsingPage />} />
             <Route path="transactions" element={<Transactions />} />
             <Route path="accounts" element={<Accounts />} />
             <Route path="analytics" element={<Analytics />} />
             <Route path="review" element={<ReviewPage />} />
        </Route>

        <Route path="*" element={<div style={{ padding: '20px', color: 'white' }}>404 - Not Found ({window.location.pathname})</div>} />
      </Routes>

      {/* Chatbot - Hidden during setup/welcome screens */}
      {user && hasModules === true && hasIdentifiers === true && !roleLoading && role !== 'QC' && (
        <LedgerBuddy />
      )}
    </ParsingProvider>
  );
}

export default App;
