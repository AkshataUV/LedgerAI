import React from 'react';
import { View, StyleSheet, StatusBar } from 'react-native';
import { useAuth } from '../shared/hooks/useAuth';
import AuthPage from './src/components/AuthPage';
import Dashboard from './src/components/Dashboard';

export default function App() {
  const { user } = useAuth();

  return (
    <View style={styles.container}>
      <StatusBar barStyle="light-content" backgroundColor="#0A0E17" />
      {user ? <Dashboard user={user} /> : <AuthPage />}
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: '#0A0E17',
  },
});
