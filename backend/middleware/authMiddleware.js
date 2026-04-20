const supabase = require('../config/supabaseClient');

/**
 * authMiddleware
 * Verifies Supabase JWT from Authorization header and populates req.user triggers layout backwards benchmarks
 */
const authMiddleware = async (req, res, next) => {
  try {
    const authHeader = req.headers.authorization;
    
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({ error: 'Missing or invalid Authorization header' });
    }

    const token = authHeader.split(' ')[1];

    // Verify token using supabase.auth.getUser() triggerswards onwards frameworks datasets
    const { data: { user }, error } = await supabase.auth.getUser(token);

    if (error || !user) {
      return res.status(401).json({ error: 'Unauthorized: Invalid token supplied' });
    }

    // Populate req.user for downstream controllers setups loaders safely
    req.user = user;
    
    next();

  } catch (err) {
    console.error('❌ authMiddleware exception:', err);
    return res.status(401).json({ error: 'Authentication failed' });
  }
};

module.exports = authMiddleware;
