import React, { useState, useEffect } from 'react';
import { useOutletContext } from 'react-router-dom';
import { supabase } from '../shared/supabase';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:3000';

const CAT_COLORS = {
  'MANUAL': '#3b82f6',     // Blue
  'G_RULE': '#10b981',     // Green
  'FILTER': '#f59e0b', // Orange
  'P_EXACT': '#8b5cf6',  // Purple
  'P_VEC': '#a855f7', // Violet
  'G_VEC': '#ec4899',   // Pink
  'LLM': '#ef4444',  // Red
  'UNKNOWN': 'var(--text-secondary)'          // Gray
};

const BADGE_COLORS = {
  'CORE': { bg: 'rgba(14, 165, 233, 0.1)', text: '#38bdf8', border: 'rgba(14, 165, 233, 0.3)' },
  'INDIVIDUAL': { bg: 'rgba(16, 185, 129, 0.1)', text: '#34d399', border: 'rgba(16, 185, 129, 0.3)' },
  'BUSINESS': { bg: 'rgba(245, 158, 11, 0.1)', text: '#fbbf24', border: 'rgba(245, 158, 11, 0.3)' }
};

const ICONS = {
  Trash: (props) => (
    <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...props}><path d="M3 6h18m-2 0v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6m3 0V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2M8 11v6M16 11v6" /></svg>
  ),
  Edit: (props) => (
    <svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...props}><path d="M17 3a2.828 2.828 0 1 1 4 4L7.5 20.5 2 22l1.5-5.5L17 3z" /></svg>
  ),
  Search: (props) => (
    <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...props}><circle cx="11" cy="11" r="8" /><path d="m21 21-4.3-4.3" /></svg>
  ),
  Plus: (props) => (
    <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#4b5563" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...props}><path d="M12 5v14M5 12h14" /></svg>
  ),
  Lightning: (props) => (
    <svg xmlns="http://www.w3.org/2000/svg" width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="var(--border-color)" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...props}><path d="M13 2 3 14h9l-1 8 10-12h-9Z" /></svg>
  )
};

const QCPanel = () => {
  const { handleLogout, user, toggleTheme, isDarkMode } = useOutletContext() || {};
  const [stats, setStats] = useState({});
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [activeTab, setActiveTab] = useState('Overview');
  const [modules, setModules] = useState([]);
  const [selectedModule, setSelectedModule] = useState(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [moduleFilter, setModuleFilter] = useState('ALL');
  const [templates, setTemplates] = useState([]);

  const [isDeleteModalOpen, setIsDeleteModalOpen] = useState(false);
  const [moduleToDelete, setModuleToDelete] = useState(null);
  const [deleteConfirmText, setDeleteConfirmText] = useState('');
  const [isDeleting, setIsDeleting] = useState(false);
  const [isProfileOpen, setIsProfileOpen] = useState(false);

  // Edit Profile States
  const [isEditProfileModalOpen, setIsEditProfileModalOpen] = useState(false);
  const [fullName, setFullName] = useState('');
  const [newName, setNewName] = useState('');
  const [oldPassword, setOldPassword] = useState('');
  const [newPassword, setNewPassword] = useState('');
  const [confirmNewPassword, setConfirmNewPassword] = useState('');
  const [profileError, setProfileError] = useState('');
  const [isUpdatingProfile, setIsUpdatingProfile] = useState(false);

  useEffect(() => {
    const fetchProfile = async () => {
      const { data } = await supabase.from('profiles').select('full_name').eq('id', user?.id).maybeSingle();
      if (data?.full_name) {
        setFullName(data.full_name);
        setNewName(data.full_name);
      }
    };
    if (user) fetchProfile();
  }, [user]);

  const handleUpdateProfile = async () => {
     setProfileError('');
     setIsUpdatingProfile(true);

     try {
         if (newName && newName !== fullName) {
            const { error: profErr } = await supabase.from('profiles').update({ full_name: newName }).eq('id', user.id);
            if (profErr) throw profErr;
            setFullName(newName);
         }

         if (oldPassword || newPassword || confirmNewPassword) {
            if (!oldPassword || !newPassword || !confirmNewPassword) {
               throw new Error('Please fill all password fields.');
            }
            if (newPassword !== confirmNewPassword) {
               throw new Error('New passwords do not match.');
            }

            const { error: signInErr } = await supabase.auth.signInWithPassword({ email: user.email, password: oldPassword });
            if (signInErr) throw new Error('Incorrect old password.');

            const { error: passErr } = await supabase.auth.updateUser({ password: newPassword });
            if (passErr) throw passErr;
         }

         setIsEditProfileModalOpen(false);
         setOldPassword('');
         setNewPassword('');
         setConfirmNewPassword('');
     } catch (err) {
         setProfileError(err.message || 'Update failed.');
     } finally {
         setIsUpdatingProfile(false);
     }
  };

  useEffect(() => {
    const fetchStats = async () => {
      try {
        const { data, error } = await supabase
          .from('transactions')
          .select('categorised_by');

        if (error) throw error;

        const grouped = (data || []).reduce((acc, curr) => {
          const cat = curr.categorised_by || 'UNKNOWN';
          acc[cat] = (acc[cat] || 0) + 1;
          return acc;
        }, {});

        setStats(grouped);
        setTotal(Object.values(grouped).reduce((a, b) => a + b, 0));
      } catch (err) {
        console.error('Failed to fetch QC stats:', err);
      } finally {
        setLoading(false);
      }
    };

    fetchStats();
  }, []);

  useEffect(() => {
    if (activeTab === 'COA Library' && modules.length === 0) {
      const fetchModules = async () => {
        try {
          const { data, error } = await supabase
            .from('coa_modules')
            .select('*');
          if (error) throw error;

          const { data: tmpData } = await supabase.from('coa_templates').select('module_id');
          const counts = (tmpData || []).reduce((acc, curr) => {
            acc[curr.module_id] = (acc[curr.module_id] || 0) + 1;
            return acc;
          }, {});

          const categoryOrder = { 'CORE': 1, 'INDIVIDUAL': 2, 'BUSINESS': 3 };
          const sorted = (data || []).sort((a, b) => {
            const orderA = categoryOrder[a.category] || 99;
            const orderB = categoryOrder[b.category] || 99;
            if (orderA !== orderB) return orderA - orderB;
            return a.module_name.localeCompare(b.module_name);
          }).map(m => ({ ...m, accountCount: counts[m.module_id] || 0 }));

          setModules(sorted);
          if (sorted.length > 0) setSelectedModule(sorted[0]);
        } catch (err) {
          console.error('Failed to fetch COA Modules:', err);
        }
      };
      fetchModules();
    }
  }, [activeTab]);

  useEffect(() => {
    if (selectedModule) {
      const fetchTemplates = async () => {
        try {
          const coreIds = modules.filter(m => m.category === 'CORE').map(m => m.module_id);
          const idsToFetch = [...new Set([selectedModule.module_id, ...coreIds])];

          const { data, error } = await supabase
            .from('coa_templates')
            .select('*')
            .in('module_id', idsToFetch);
          if (error) throw error;

          const buildHierarchy = (items) => {
            const map = {};
            items.forEach(item => {
              map[item.template_id] = { ...item, children: [] };
            });
            const roots = [];
            items.forEach(item => {
              if (item.parent_template_id && map[item.parent_template_id]) {
                map[item.parent_template_id].children.push(map[item.template_id]);
              } else {
                roots.push(map[item.template_id]);
              }
            });
            return roots;
          };

          const sortTree = (nodes) => {
            return nodes.sort((a, b) => a.account_name.localeCompare(b.account_name)).map(n => ({
              ...n,
              children: sortTree(n.children)
            }));
          };

          setTemplates(sortTree(buildHierarchy(data || [])));
        } catch (err) {
          console.error('Failed to fetch Templates:', err);
        }
      };
      fetchTemplates();
    } else {
      setTemplates([]);
    }
  }, [selectedModule, modules]);

  const renderTemplateRow = (tmp, depth = 0) => {
    const isCoreTemplate = modules.some(m => m.module_id === tmp.module_id && m.category === 'CORE');
    return (
      <React.Fragment key={tmp.template_id}>
        <div style={{ ...styles.gridRow, paddingLeft: `${20 + depth * 24}px` }}>
          <div style={{
            fontWeight: depth === 0 ? '700' : '500',
            color: depth === 0 ? 'var(--text-primary)' : 'var(--text-primary)',
            display: 'flex',
            alignItems: 'center',
            gap: '6px'
          }}>
            {depth > 0 && <span style={{ color: '#4b5563', fontSize: '12px' }}>└─</span>}
            {tmp.account_name}
            {isCoreTemplate && <span style={styles.coreCardBadge}>CORE</span>}
          </div>
          <div style={{ color: 'var(--text-secondary)', fontSize: '11px' }}>{tmp.account_type || '-'}</div>
          <div style={{ color: 'var(--text-secondary)', fontSize: '11px' }}>{tmp.balance_nature || '-'}</div>
        </div>
        {tmp.children && tmp.children.map(child => renderTemplateRow(child, depth + 1))}
      </React.Fragment>
    );
  };

  const handleDeleteModule = async () => {
    if (!moduleToDelete) return;
    setIsDeleting(true);
    try {
      const { data: { session } } = await supabase.auth.getSession();
      if (!session?.access_token) {
        throw new Error('Authentication required. Please sign in again.');
      }

      const response = await fetch(`${API_BASE_URL}/api/qc/modules/${moduleToDelete.module_id}`, {
        method: 'DELETE',
        headers: {
          Authorization: `Bearer ${session.access_token}`
        }
      });

      if (!response.ok) {
        const errorPayload = await response.json().catch(() => ({}));
        throw new Error(errorPayload.error || 'Failed to delete module.');
      }

      setModules(modules.filter(m => m.module_id !== moduleToDelete.module_id));
      if (selectedModule?.module_id === moduleToDelete.module_id) setSelectedModule(modules.find(m => m.module_id !== moduleToDelete.module_id) || null);
      setIsDeleteModalOpen(false);
      setModuleToDelete(null);
      setDeleteConfirmText('');
    } catch (err) {
      console.error('Delete failed:', err);
      alert('Delete failed: ' + (err.message || err));
    } finally {
      setIsDeleting(false);
    }
  };

  return (
    <div style={styles.container}>
      {isDeleteModalOpen && moduleToDelete && (
        <div style={styles.modalOverlay}>
          <div style={styles.modalContent}>
            <div style={styles.modalHeader}>
              <div>
                <div style={styles.modalTitle}>Delete Module</div>
                <div style={styles.modalSubtitle}>CRITICAL SECURITY CHECK</div>
              </div>
              <span style={styles.modalClose} onClick={() => setIsDeleteModalOpen(false)}>×</span>
            </div>
            <div style={styles.modalBody}>
              <div style={styles.warningCircle}>
                <svg xmlns="http://www.w3.org/2000/svg" width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="#ef4444" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z" /><line x1="12" y1="9" x2="12" y2="13" /><line x1="12" y1="17" x2="12.01" y2="17" /></svg>
              </div>
              <p style={styles.modalMainText}>You are about to delete <strong style={{ color: '#ef4444' }}>"{moduleToDelete.module_name}"</strong></p>
              <p style={styles.modalSubText}>This action is permanent and will remove all associated account templates.</p>

              <div style={styles.modalInputLabel}>TYPE <span style={{ color: '#ef4444' }}>"{moduleToDelete.module_name}"</span> TO CONFIRM</div>
              <input
                type="text"
                placeholder="Confirm module name..."
                style={styles.modalInput}
                value={deleteConfirmText}
                onChange={(e) => setDeleteConfirmText(e.target.value)}
              />

              <div style={styles.modalActions}>
                <button style={styles.cancelBtn} onClick={() => setIsDeleteModalOpen(false)}>CANCEL</button>
                <button
                  style={{
                    ...styles.confirmBtn,
                    opacity: deleteConfirmText === moduleToDelete.module_name && !isDeleting ? 1 : 0.5,
                    cursor: deleteConfirmText === moduleToDelete.module_name && !isDeleting ? 'pointer' : 'not-allowed',
                    backgroundColor: deleteConfirmText === moduleToDelete.module_name && !isDeleting ? '#ef4444' : 'var(--text-primary)',
                    color: deleteConfirmText === moduleToDelete.module_name && !isDeleting ? '#ffffff' : 'var(--text-secondary)'
                  }}
                  disabled={deleteConfirmText !== moduleToDelete.module_name || isDeleting}
                  onClick={handleDeleteModule}
                >
                  {isDeleting ? 'DELETING...' : 'CONFIRM DESTRUCTION'}
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
      {isEditProfileModalOpen && (
        <div style={styles.modalOverlay}>
          <div style={{ ...styles.modalContent, maxWidth: '400px' }}>
            <div style={styles.modalHeader}>
              <div>
                <div style={styles.modalTitle}>Edit Profile</div>
                <div style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>{user?.email}</div>
              </div>
              <span style={styles.modalClose} onClick={() => setIsEditProfileModalOpen(false)}>×</span>
            </div>
            <div style={{ padding: '20px', display: 'flex', flexDirection: 'column', gap: '16px' }}>
              {profileError && <div style={{ color: '#ef4444', backgroundColor: 'rgba(239, 68, 68, 0.1)', padding: '10px', borderRadius: '8px', fontSize: '11px', border: '1px solid rgba(239, 68, 68, 0.2)' }}>{profileError}</div>}
              
              <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
                <label style={{ fontSize: '11px', color: 'var(--text-secondary)', fontWeight: '800' }}>FULL NAME</label>
                <input type="text" value={newName} placeholder="Group Reviewer" onChange={(e) => setNewName(e.target.value)} style={styles.modalInput} />
              </div>

              <div style={{ height: '1px', backgroundColor: 'var(--border-color)', margin: '4px 0', opacity: 0.3 }} />
              
              <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
                <label style={{ fontSize: '11px', color: 'var(--text-secondary)', fontWeight: '800' }}>OLD PASSWORD</label>
                <input type="password" value={oldPassword} placeholder="••••••••" onChange={(e) => setOldPassword(e.target.value)} style={styles.modalInput} />
              </div>

              <div style={{ display: 'flex', gap: '12px' }}>
                <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: '4px' }}>
                  <label style={{ fontSize: '11px', color: 'var(--text-secondary)', fontWeight: '800' }}>NEW PASSWORD</label>
                  <input type="password" value={newPassword} placeholder="••••••••" onChange={(e) => setNewPassword(e.target.value)} style={styles.modalInput} />
                </div>
                <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: '4px' }}>
                  <label style={{ fontSize: '11px', color: 'var(--text-secondary)', fontWeight: '800' }}>CONFIRM</label>
                  <input type="password" value={confirmNewPassword} placeholder="••••••••" onChange={(e) => setConfirmNewPassword(e.target.value)} style={styles.modalInput} />
                </div>
              </div>

              <div style={styles.modalActions}>
                <button style={styles.cancelBtn} onClick={() => setIsEditProfileModalOpen(false)}>CANCEL</button>
                <button 
                  onClick={handleUpdateProfile} 
                  disabled={isUpdatingProfile} 
                  style={{ ...styles.confirmBtn, backgroundColor: 'var(--accent-color)', color: '#FFFFFF', opacity: isUpdatingProfile ? 0.7 : 1 }}
                >
                  {isUpdatingProfile ? 'SAVING...' : 'SAVE CHANGES'}
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
      <header style={{
        display: 'flex',
        width: '100%',
        backgroundColor: 'var(--bg-secondary)',
        borderBottom: '1px solid var(--border-color)',
        padding: '16px 24px',
        alignItems: 'flex-start',
        justifyContent: 'space-between',
        gap: '24px',
        boxSizing: 'border-box'
      }}>
        {/* Left: Titles Column */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: '2px', flexShrink: 0 }}>
          <div style={{ fontSize: '13px', fontWeight: '900', color: 'var(--accent-color)', letterSpacing: '0.5px' }}>LEDGERAI</div>
          <div style={{ fontSize: '16px', fontWeight: '800', color: 'var(--text-primary)', lineHeight: '1.2' }}>Quality</div>
          <div style={{ fontSize: '16px', fontWeight: '800', color: 'var(--text-primary)', lineHeight: '1.2' }}>Control</div>
        </div>

        {/* Center: Stats & Legends */}
        <div style={{ flexGrow: 1, display: 'flex', flexDirection: 'column', gap: '15px', minWidth: 0 }}>
          {/* Progress Bar line top-stretched */}
          <div style={{ display: 'flex', alignItems: 'center', width: '100%' }}>
            <div style={{ ...styles.barContainer, height: '15px', width: '100%' }}>
              {total === 0 ? (
                <div style={{ ...styles.barSegment, width: '100%', backgroundColor: 'var(--border-color)' }} />
              ) : (
                Object.entries(stats).map(([cat, count]) => (
                  <div
                    key={cat}
                    style={{ ...styles.barSegment, width: (count / total) * 100 + '%', backgroundColor: CAT_COLORS[cat] || CAT_COLORS.UNKNOWN }}
                    title={`${cat}: ${count}`}
                  />
                ))
              )}
            </div>
          </div>

          {/* Legends Row with larger font */}
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: '8px', rowGap: '6px', width: '100%' }}>
            {Object.entries(CAT_COLORS).map(([cat, color]) => {
              const count = stats[cat] || 0;
              return (
                <div key={cat} style={{ display: 'flex', alignItems: 'center', justifyContent:'center', gap: '6px', padding: '4px 8px', backgroundColor: 'var(--bg-primary)', borderRadius: '6px', border: '1px solid var(--border-color)', flex: '1 1 0px', minWidth: 'fit-content', whiteSpace: 'nowrap'}}>
                  <div style={{ width: '10px', height: '10px', borderRadius: '50%', backgroundColor: color }} />
                  <span style={{ fontSize: '13px', color: '#9AA3AF', textTransform: 'capitalize', fontWeight: '500' }}>
                    {cat.replace(/_/g, ' ').toLowerCase()} <strong style={{ color: 'var(--text-primary)', marginLeft: '3px', fontWeight: '800' }}>{count}</strong>
                  </span>
                </div>
              );
            })}
          </div>
        </div>

        {/* Right: Profile Dropdown Menu */}
        <div style={{ position: 'relative', flexShrink: 0 }}>
          <div
            onClick={() => setIsProfileOpen(!isProfileOpen)}
            style={{ width: '38px', height: '38px', borderRadius: '50%', backgroundColor: 'var(--border-color)', border: '1.5px solid var(--accent-color)', display: 'flex', alignItems: 'center', justifyContent: 'center', cursor: 'pointer', color: 'var(--text-primary)', fontWeight: '800', fontSize: '14px', transition: 'all 0.2s' }}
          >
            {user?.email ? user.email[0].toUpperCase() : 'A'}
          </div>
          {isProfileOpen && (
            <div style={{ position: 'absolute', top: '48px', right: 0, backgroundColor: 'var(--bg-secondary)', border: '1px solid var(--border-color)', borderRadius: '12px', width: '160px', overflow: 'hidden', boxShadow: '0 10px 30px rgba(0,0,0,0.5)', zIndex: 1000 }}>
              <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--border-color)', fontSize: '11px', color: '#9AA3AF' }}>QC Admin</div>
              <button onClick={toggleTheme} style={{ width: '100%', padding: '12px 16px', backgroundColor: 'transparent', border: 'none', color: 'var(--text-primary)', fontSize: '12px', textAlign: 'left', cursor: 'pointer' }}>{isDarkMode ? '☀️ Light Mode' : '🌙 Dark Mode'}</button>
              <button onClick={() => { setIsEditProfileModalOpen(true); setIsProfileOpen(false); }} style={{ width: '100%', padding: '12px 16px', backgroundColor: 'transparent', border: 'none', color: 'var(--text-primary)', fontSize: '12px', textAlign: 'left', cursor: 'pointer' }}>👤 Edit Profile</button>
              <button onClick={handleLogout} style={{ width: '100%', padding: '12px 16px', backgroundColor: 'transparent', border: 'none', color: '#A63D40', fontSize: '12px', fontWeight: '700', textAlign: 'left', cursor: 'pointer', borderTop: '1px solid var(--border-color)' }}>🚪 Logout</button>
            </div>
          )}
        </div>
      </header>

      {/* Nav Bar below the complete top bar */}
      <nav style={{ display: 'flex', gap: '8px', padding: '12px 24px', backgroundColor: 'var(--bg-primary)', borderBottom: '1px solid var(--border-color)' }}>
        {['COA Library', 'Audit Queue', 'Rules Config', 'Vector Cache'].map(tab => (
          <button
            key={tab}
            onClick={() => setActiveTab(tab)}
            style={{
              backgroundColor: activeTab === tab ? 'rgba(127, 175, 138, 0.08)' : 'transparent',
              border: activeTab === tab ? '1.5px solid var(--accent-color)' : '1px solid var(--border-color)',
              color: activeTab === tab ? 'var(--accent-color)' : '#9AA3AF',
              fontSize: '10.5px',
              fontWeight: '800',
              padding: '6px 14px',
              borderRadius: '12px',
              cursor: 'pointer',
              letterSpacing: '0.25px',
              textTransform: 'uppercase',
              transition: 'all 0.15s'
            }}
          >
            {tab === 'COA Library' ? 'GLOBAL' : tab === 'Audit Queue' ? 'USER' : tab.replace(' Config', '')}
          </button>
        ))}
      </nav>

      {/* COA Library Tab Content */}
      {activeTab === 'COA Library' && (
        <div style={styles.tabContent}>
          <aside style={styles.sidebar}>
            <div style={styles.sidebarHeader}>
              <span>MODULES</span>
              <span style={styles.totalBadge}>{modules.length} Total</span>
            </div>

            <div style={styles.searchRow}>
              <div style={styles.searchIconInput}>
                <ICONS.Search style={styles.searchIcon} />
                <input
                  type="text"
                  placeholder="Search name..."
                  style={styles.searchInput}
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                />
              </div>
              <select
                style={styles.filterBtn}
                value={moduleFilter}
                onChange={(e) => setModuleFilter(e.target.value)}
              >
                <option value="ALL" style={styles.selectOption}>ALL</option>
                <option value="INDIVIDUAL" style={styles.selectOption}>INDIVIDUAL</option>
                <option value="BUSINESS" style={styles.selectOption}>BUSINESS</option>
              </select>
            </div>

            <div style={styles.moduleList}>
              {/* Add Module Button Card */}
              <div style={styles.addModuleCard}>
                <ICONS.Plus />
                <div style={styles.addModuleText}>ADD MODULE</div>
              </div>

              {modules
                .filter(mod => mod.module_name.toLowerCase().includes(searchQuery.toLowerCase()))
                .filter(mod => moduleFilter === 'ALL' || mod.category === moduleFilter)
                .map(mod => {
                  const isActive = selectedModule?.module_id === mod.module_id;
                  return (
                    <div
                      key={mod.module_id}
                      onClick={() => setSelectedModule(mod)}
                      style={isActive ? styles.moduleCardActive : styles.moduleCard}
                    >
                      <div style={styles.cardHeader}>
                        <div>
                          <div style={styles.moduleName}>{mod.module_name}</div>
                          <div style={styles.moduleSubtitle}>{mod.accountCount || 0} Accounts</div>
                        </div>
                        <div style={styles.headerActions}>
                          <span style={{
                            ...styles.moduleBadge,
                            backgroundColor: (BADGE_COLORS[mod.category]?.bg || '#1c2230'),
                            color: (BADGE_COLORS[mod.category]?.text || 'var(--text-primary)'),
                            borderColor: (BADGE_COLORS[mod.category]?.border || 'var(--border-color)')
                          }}>
                            {mod.category}
                          </span>
                        </div>
                      </div>

                      {isActive && (
                        <>
                          {mod.category !== 'CORE' && (
                            <div style={styles.cardFabDelete} onClick={(e) => { e.stopPropagation(); setModuleToDelete(mod); setIsDeleteModalOpen(true); setDeleteConfirmText(''); }}>
                              <ICONS.Trash style={{ color: '#ef4444', width: '12px', height: '12px' }} />
                            </div>
                          )}
                          <div style={styles.cardSeparator} />
                          <div style={styles.descHeader}>DESCRIPTION</div>
                          <div style={styles.moduleDescRow}>
                            <div style={styles.moduleDescActive}>{mod.description || 'No description listed.'}</div>
                            <span style={styles.editIconActive} onClick={(e) => { e.stopPropagation(); alert('Edit Desc'); }}>
                              <ICONS.Edit style={{ color: '#38bdf8', width: '11px', height: '11px' }} />
                            </span>
                          </div>
                        </>
                      )}
                    </div>
                  );
                })}
            </div>
          </aside>

          <div style={styles.mainContent}>
            {!selectedModule ? (
              <div style={styles.emptyState}>
                <ICONS.Lightning style={styles.emptyStateIcon} />
                <p style={styles.emptyStateText}>SELECT A MODULE FROM THE LEFT <br /> TO VIEW LIBRARY TEMPLATES</p>
              </div>
            ) : (
              <div style={styles.insetCard}>
                <div style={styles.insetHeader}>
                  <h2 style={{ marginBottom: '4px', fontSize: '18px', color: '#ffffff' }}>{selectedModule.module_name}</h2>
                  <p style={{ color: 'var(--text-secondary)', fontSize: '12px', marginBottom: '24px' }}>{selectedModule.description || 'No description listed.'}</p>
                </div>

                <div style={styles.templateListGrid}>
                  <div style={styles.gridHeader}>
                    <div>ACCOUNT NAME</div>
                    <div>TYPE</div>
                    <div>BALANCE</div>
                  </div>
                  <div style={styles.gridRows}>
                    {templates.length === 0 ? (
                      <div style={{ padding: '32px', color: 'var(--text-secondary)', fontSize: '12px', textAlign: 'center' }}>No templates linked to this module.</div>
                    ) : (
                      templates.map(tmp => renderTemplateRow(tmp))
                    )}
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
};

const styles = {
  container: {
    display: 'flex',
    flexDirection: 'column',
    backgroundColor: 'var(--bg-primary)',
    color: 'var(--text-primary)',
    fontFamily: 'system-ui, -apple-system, sans-serif',
    height: '100vh' // Contain view heights accurately forwards downwards triggersward outwards downwards onwards forwards upwards downwards outwards download.
  },
  header: {
    height: '10vh',
    minHeight: '10vh',
    maxHeight: '10vh',
    overflow: 'hidden',
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    gap: '40px',
    padding: '12px 24px',
    borderBottom: '1px solid var(--border-color)',
    boxSizing: 'border-box'
  },
  navBar: {
    display: 'flex',
    gap: '8px',
    padding: '8px 24px',
    backgroundColor: '#0a0d16',
    borderBottom: '1px solid var(--border-color)'
  },
  navBtn: {
    padding: '6px 12px',
    borderRadius: '4px',
    border: 'none',
    backgroundColor: 'transparent',
    color: 'var(--text-secondary)',
    cursor: 'pointer',
    fontSize: '12px',
    transition: 'all 0.2s'
  },
  navBtnActive: {
    padding: '6px 12px',
    borderRadius: '4px',
    border: 'none',
    backgroundColor: 'var(--border-color)',
    color: 'var(--text-primary)',
    fontWeight: '500',
    cursor: 'pointer',
    fontSize: '12px'
  },
  headerTitle: {
    flexShrink: 0
  },
  h1: {
    margin: 0,
    fontSize: '18px',
    fontWeight: '600',
    background: 'linear-gradient(to right, #60a5fa, #34d399)',
    WebkitBackgroundClip: 'text',
    WebkitTextFillColor: 'transparent',
  },
  subtitle: {
    margin: '2px 0 0 0',
    color: 'var(--text-secondary)',
    fontSize: '12px'
  },
  statsWrapper: {
    flexGrow: 1,
    display: 'flex',
    flexDirection: 'column',
    gap: '8px',
  },
  barContainer: {
    display: 'flex',
    height: '14px',
    width: '100%',
    borderRadius: '6px',
    overflow: 'hidden',
    backgroundColor: 'var(--border-color)',
  },
  tabContent: {
    display: 'flex',
    flexGrow: 1,
    overflow: 'hidden',
    padding: '16px',
    backgroundColor: '#0a0d15'
  },
  sidebar: {
    width: '280px',
    paddingRight: '16px',
    display: 'flex',
    flexDirection: 'column',
    boxSizing: 'border-box'
  },
  sidebarHeader: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: '16px',
    fontWeight: '800',
    color: 'var(--text-secondary)',
    fontSize: '11px',
    letterSpacing: '1px'
  },
  totalBadge: {
    fontSize: '10px',
    color: 'var(--text-secondary)',
    fontWeight: 'normal'
  },
  searchRow: {
    display: 'flex',
    gap: '8px',
    marginBottom: '20px'
  },
  searchIconInput: {
    display: 'flex',
    alignItems: 'center',
    backgroundColor: '#141721',
    borderRadius: '8px',
    padding: '6px 10px',
    flexGrow: 1,
    border: '1px solid var(--border-color)'
  },
  searchIcon: {
    color: '#4b5563',
    fontSize: '12px',
    marginRight: '6px'
  },
  searchInput: {
    border: 'none',
    backgroundColor: 'transparent',
    color: 'var(--text-primary)',
    fontSize: '11px',
    width: '100%',
    outline: 'none'
  },
  filterBtn: {
    display: 'flex',
    alignItems: 'center',
    padding: '6px 12px',
    backgroundColor: '#141721',
    border: '1px solid var(--border-color)',
    borderRadius: '8px',
    color: 'var(--text-secondary)',
    fontSize: '11px',
    cursor: 'pointer',
    outline: 'none'
  },
  selectOption: {
    backgroundColor: '#141721',
    color: 'var(--text-primary)'
  },
  moduleList: {
    flexGrow: 1,
    display: 'flex',
    flexDirection: 'column',
    gap: '12px',
    overflowY: 'auto'
  },
  addModuleCard: {
    border: '1px dashed #272e3f',
    borderRadius: '12px',
    padding: '24px',
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    justifyContent: 'center',
    cursor: 'pointer',
    backgroundColor: 'transparent',
    aspectRatio: '16 / 9',
  },
  plusIcon: {
    fontSize: '24px',
    color: 'var(--text-secondary)',
    marginBottom: '6px'
  },
  addModuleText: {
    fontSize: '9px',
    color: 'var(--text-secondary)',
    fontWeight: '800',
    letterSpacing: '0.5px'
  },
  moduleCard: {
    backgroundColor: '#141721',
    border: '1px solid var(--border-color)',
    borderRadius: '16px',
    padding: '16px',
    cursor: 'pointer',
    position: 'relative',
    display: 'flex',
    flexDirection: 'column',
    gap: '8px',
    transition: 'all 0.2s'
  },
  moduleCardActive: {
    backgroundColor: '#1d2230', // elevated dark slate
    border: '1px solid #0ea5e9', // theme accent border
    borderRadius: '16px',
    padding: '16px',
    cursor: 'pointer',
    position: 'relative',
    display: 'flex',
    flexDirection: 'column',
    boxShadow: '0 4px 20px rgba(14, 165, 233, 0.08)'
  },
  cardHeader: {
    display: 'flex',
    alignItems: 'flex-start',
    justifyContent: 'space-between',
    gap: '8px'
  },
  moduleBadge: {
    backgroundColor: '#1c2230',
    color: '#34d399',
    fontSize: '8px',
    fontWeight: '800',
    padding: '3px 8px',
    borderRadius: '20px',
    textTransform: 'uppercase',
    letterSpacing: '0.5px',
    border: '1px solid var(--border-color)',
    position: 'absolute',
    right: '16px',
    top: '12px'
  },
  moduleName: {
    fontSize: '14px',
    color: 'var(--text-primary)',
    fontWeight: '700'
  },
  moduleSubtitle: {
    fontSize: '11px',
    color: 'var(--text-secondary)',
    marginTop: '2px'
  },
  moduleDescActive: {
    fontSize: '12px',
    color: 'var(--text-primary)',
    lineHeight: '1.4',
    flexGrow: 1
  },
  cardFabDelete: {
    position: 'absolute',
    right: '16px',
    top: '36px', // underneath the Badge absolutely without overlapping nodes
    backgroundColor: 'rgba(239, 68, 68, 0.08)',
    width: '24px',
    height: '24px',
    borderRadius: '50%',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    cursor: 'pointer',
    border: '1px solid rgba(239, 68, 68, 0.15)'
  },
  cardSeparator: {
    height: '1px',
    backgroundColor: '#1c2231',
    margin: '12px 0 8px 0',
    borderBottom: '1px solid var(--border-color)'
  },
  descHeader: {
    fontSize: '9px',
    fontWeight: '800',
    color: '#4b5563',
    letterSpacing: '0.6px',
    marginBottom: '4px'
  },
  moduleDescRow: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
    gap: '8px'
  },
  editIconActive: {
    cursor: 'pointer',
    backgroundColor: 'rgba(14, 165, 233, 0.08)',
    width: '24px',
    height: '24px',
    borderRadius: '50%',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    flexShrink: 0,
    border: '1px solid rgba(14, 165, 233, 0.15)'
  },
  mainContent: {
    flexGrow: 1,
    backgroundColor: '#11131a',
    borderRadius: '16px',
    border: '1px solid var(--border-color)',
    overflow: 'hidden'
  },
  emptyState: {
    height: '100%',
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    justifyContent: 'center',
  },
  emptyStateIcon: {
    color: '#272e3f',
    fontSize: '36px',
    marginBottom: '16px'
  },
  emptyStateText: {
    color: 'var(--text-secondary)',
    fontSize: '11px',
    textAlign: 'center',
    lineHeight: '1.6',
    fontWeight: '800',
    letterSpacing: '0.5px'
  },
  insetCard: {
    padding: '24px',
    height: '100%',
    display: 'flex',
    flexDirection: 'column'
  },
  templateListGrid: {
    display: 'flex',
    flexDirection: 'column',
    backgroundColor: '#141721',
    borderRadius: '12px',
    border: '1px solid var(--border-color)',
    overflow: 'hidden',
    flexGrow: 1
  },
  gridHeader: {
    display: 'grid',
    gridTemplateColumns: 'minmax(220px, 1fr) 120px 120px',
    padding: '12px 20px',
    backgroundColor: '#0d1017',
    borderBottom: '1px solid var(--border-color)',
    fontSize: '10px',
    fontWeight: '800',
    color: 'var(--text-secondary)',
    letterSpacing: '0.5px'
  },
  gridRows: {
    overflowY: 'auto',
    flexGrow: 1,
    height: '0px' // correctly stretches available flex height constraints config dow forwards trimswards down dashboards downswards downwards downwards outwards downward downwards models downwards configuration upwards upwards downwards downward layout outwards downward onwards downwards designWARDS.
  },
  gridRow: {
    display: 'grid',
    gridTemplateColumns: 'minmax(220px, 1fr) 120px 120px',
    padding: '12px 20px',
    borderBottom: '1px solid #1c2230',
    alignItems: 'center',
    fontSize: '12px',
    transition: 'background-color 0.2s',
  },
  coreCardBadge: {
    backgroundColor: '#0c223c',
    color: '#38bdf8',
    fontSize: '8.5px',
    fontWeight: '800',
    padding: '2px 6px',
    borderRadius: '12px',
    border: '1px solid rgba(14, 165, 233, 0.2)',
    lineHeight: '1',
    display: 'inline-block',
    marginLeft: '6px',
    letterSpacing: '0.2px'
  },
  modalOverlay: { position: 'fixed', top: 0, left: 0, right: 0, bottom: 0, backgroundColor: 'rgba(0,0,0,0.75)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 },
  modalContent: { backgroundColor: 'var(--bg-secondary)', color: 'var(--text-primary)', width: '400px', borderRadius: '24px', overflow: 'hidden', boxShadow: '0 20px 50px rgba(0,0,0,0.5)', animation: 'slideUp 0.15s ease-out', border: '1px solid var(--border-color)' },
  modalHeader: { padding: '24px 24px 16px 24px', backgroundColor: 'var(--bg-primary)', borderBottom: '1px solid var(--border-color)', display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' },
  modalTitle: { fontSize: '18px', fontWeight: '900', color: 'var(--text-primary)' },
  modalSubtitle: { fontSize: '9px', fontWeight: '800', color: '#A63D40', letterSpacing: '0.5px', marginTop: '4px' },
  modalClose: { cursor: 'pointer', fontSize: '18px', color: '#9AA3AF', fontWeight: '800' },
  modalBody: { padding: '24px', display: 'flex', flexDirection: 'column', alignItems: 'center' },
  warningCircle: { width: '64px', height: '64px', borderRadius: '50%', backgroundColor: 'rgba(166, 61, 64, 0.1)', display: 'flex', alignItems: 'center', justifyContent: 'center', marginBottom: '20px' },
  modalMainText: { fontSize: '14px', color: 'var(--text-primary)', fontWeight: '700', marginBottom: '4px', textAlign: 'center' },
  modalSubText: { fontSize: '12px', color: '#9AA3AF', textAlign: 'center', marginBottom: '24px', lineHeight: '1.5' },
  modalInputLabel: { alignSelf: 'flex-start', fontSize: '10px', color: '#9AA3AF', fontWeight: '800', marginBottom: '6px', letterSpacing: '0.5px' },
  modalInput: { width: '100%', padding: '14px 16px', borderRadius: '12px', border: '1.5px solid var(--border-color)', backgroundColor: 'var(--bg-primary)', fontSize: '13px', outline: 'none', color: 'var(--text-primary)', marginBottom: '24px' },
  modalActions: { display: 'flex', gap: '12px', width: '100%' },
  cancelBtn: { flex: 1, padding: '14px', borderRadius: '14px', border: '1px solid var(--border-color)', backgroundColor: 'var(--bg-secondary)', color: '#9AA3AF', fontWeight: '800', fontSize: '12px', cursor: 'pointer' },
  confirmBtn: { flex: 1, padding: '14px', borderRadius: '14px', border: 'none', fontWeight: '800', fontSize: '12px', cursor: 'pointer', transition: 'all 0.2s' },
  barSegment: {
    height: '100%',
    transition: 'width 0.3s ease',
    cursor: 'pointer'
  },
  legendRow: {
    display: 'flex',
    flexWrap: 'wrap',
    gap: '12px',
    rowGap: '4px'
  },
  legendItem: {
    display: 'flex',
    alignItems: 'center',
    gap: '6px',
    padding: '4px 8px',
    backgroundColor: '#111827',
    borderRadius: '6px',
    border: '1px solid var(--border-color)'
  },
  legendDot: {
    width: '8px',
    height: '8px',
    borderRadius: '50%',
    flexShrink: 0
  },
  legendText: {
    fontSize: '11px',
    color: 'var(--text-primary)',
    textTransform: 'capitalize'
  },
  percentText: {
    color: 'var(--text-secondary)',
    fontSize: '10px'
  }
};


export default QCPanel;
