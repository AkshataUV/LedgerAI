import { useEffect, useState } from 'react';
import { supabase } from '../shared/supabase';

export function useAuth() {
  const [user, setUser] = useState(null);

  useEffect(() => {
    // 1. Get the current user session immediately
    const session = supabase.auth.getSession();
    setUser(session?.user ?? null);

    // 2. Listen for login/logout events
    const { data: listener } = supabase.auth.onAuthStateChange((event, session) => {
      setUser(session?.user ?? null);
    });

    return () => listener.subscription.unsubscribe();
  }, []);

  return { user };
}