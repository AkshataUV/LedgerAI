import { supabase } from './supabase';

/**
 * Register a new user with email, password, and full name.
 * 
 * @param {string} email
 * @param {string} password
 * @param {string} fullName
 * @returns {Promise<{data: any, error: any}>}
 */
export const signUp = async (email, password, fullName) => {
  const { data, error } = await supabase.auth.signUp({
    email,
    password,
    options: {
      data: {
        full_name: fullName, // This triggers handle_new_user to populate profiles table
      },
    },
  });
  return { data, error };
};

/**
 * Sign in an existing user.
 * 
 * @param {string} email
 * @param {string} password
 * @returns {Promise<{data: any, error: any}>}
 */
export const signIn = async (email, password) => {
  const { data, error } = await supabase.auth.signInWithPassword({
    email,
    password,
  });
  return { data, error };
};

/**
 * Sign out the current user.
 * 
 * @returns {Promise<{error: any}>}
 */
export const signOut = async () => {
  const { error } = await supabase.auth.signOut();
  return { error };
};
