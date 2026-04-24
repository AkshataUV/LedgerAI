import { createContext, useContext, useState, useEffect } from 'react';
import { supabase } from '../shared/supabase';
import { useAuth } from '../shared/hooks/useAuth';

const RoleContext = createContext(null);

export function RoleProvider({ children }) {
  const { user } = useAuth();
  const [role, setRole] = useState(null);
  const [roleLoading, setRoleLoading] = useState(true);

  useEffect(() => {
    if (!user) {
      setRole(null);
      setRoleLoading(false);
      return;
    }

    const fetchRole = async () => {
      setRoleLoading(true);
      try {
        const { data, error } = await supabase
          .from('profiles')
          .select('role')
          .eq('id', user.id)
          .single();

        if (error) throw error;
        setRole(data?.role || 'USER');
      } catch (err) {
        console.error('Error fetching role in RoleContext:', err);
        setRole('USER');
      } finally {
        setRoleLoading(false);
      }
    };

    fetchRole();
  }, [user]);

  return (
    <RoleContext.Provider value={{ role, roleLoading }}>
      {children}
    </RoleContext.Provider>
  );
}

export const useRole = () => {
  const context = useContext(RoleContext);
  if (!context) {
    throw new Error('useRole must be used within a RoleProvider');
  }
  return context;
};
