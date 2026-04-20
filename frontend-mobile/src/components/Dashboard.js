import React from 'react';
import { View, Text, TouchableOpacity, StyleSheet, SafeAreaView } from 'react-native';
import { signOut } from '../../../shared/authService';

const Dashboard = ({ user }) => {
  const handleLogout = async () => {
    try {
      const { error } = await signOut();
      if (error) console.error('Error signing out:', error.message);
    } catch (err) {
      console.error('Logout failed:', err);
    }
  };

  return (
    <SafeAreaView style={styles.container}>
      <View style={styles.topBar}>
        <Text style={styles.logoText}>
          ▲ Ledger<Text style={styles.logoAccent}>AI</Text>
        </Text>
        <TouchableOpacity style={styles.logoutBtn} onPress={handleLogout}>
          <Text style={styles.logoutBtnText}>Logout</Text>
        </TouchableOpacity>
      </View>

      {user ? (
        <View style={styles.content}>
          <Text style={styles.welcomeText}>Logged in as</Text>
          <Text style={styles.userEmail}>{user.email}</Text>
        </View>
      ) : null}
    </SafeAreaView>
  );
};

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: '#0A0E17',
  },
  topBar: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    paddingHorizontal: 20,
    paddingVertical: 16,
    backgroundColor: 'rgba(30, 41, 59, 0.4)',
    borderBottomWidth: 1,
    borderColor: 'rgba(255, 255, 255, 0.05)',
  },
  logoText: {
    fontSize: 20,
    fontWeight: '800',
    color: '#F8FAFC',
  },
  logoAccent: {
    color: '#6366F1',
  },
  logoutBtn: {
    backgroundColor: 'rgba(239, 68, 68, 0.1)',
    borderWidth: 1,
    borderColor: 'rgba(239, 68, 68, 0.2)',
    borderRadius: 12,
    paddingVertical: 8,
    paddingHorizontal: 16,
  },
  logoutBtnText: {
    color: '#FCA5A5',
    fontWeight: '600',
    fontSize: 14,
  },
  content: {
    flex: 1,
    justifyContent: 'center',
    alignItems: 'center',
    padding: 40,
  },
  welcomeText: {
    fontSize: 14,
    color: '#94A3B8',
    textTransform: 'uppercase',
    letterSpacing: 1,
    marginBottom: 8,
  },
  userEmail: {
    fontSize: 24,
    fontWeight: '800',
    color: '#F8FAFC',
    textAlign: 'center',
  },
});

export default Dashboard;
