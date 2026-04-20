const supabase = require('../config/supabaseClient');

/**
 * requireQC
 * Verifies if the authenticated user has QC or ADMIN privileges based on profiles table
 */
const requireQC = async (req, res, next) => {
  try {
    if (!req.user || !req.user.id) {
      return res.status(401).json({ error: 'Unauthorized: User not found in request' });
    }

    const { data: profile, error } = await supabase
      .from('profiles')
      .select('role')
      .eq('id', req.user.id)
      .single();

    if (error || !profile) {
      console.error('❌ requireQC query failure:', error);
      return res.status(403).json({ error: 'Access denied. Account profile not resolved.' });
    }

    const role = profile.role?.toUpperCase();

    if (role === 'QC' || role === 'ADMIN') {
      return next(); // Pass verification
    }

    return res.status(403).json({ error: 'Access denied. QC privileges required.' });

  } catch (err) {
    console.error('❌ requireQC exception:', err);
    return res.status(500).json({ error: 'Internal server safety error verification failed.' });
  }
};

module.exports = { requireQC };
