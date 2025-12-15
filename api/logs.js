import { kv } from '@vercel/kv';

export default async function handler(req, res) {
  try {
    // Ambil logs global
    const logs = await kv.lrange('fish_logs', 0, 50);
    
    // Ambil daftar user aktif
    const userIds = await kv.smembers('active_users');
    
    // Ambil detail stats setiap user
    let accounts = [];
    if (userIds.length > 0) {
        // Pipeline request agar cepat
        const pipeline = kv.pipeline();
        userIds.forEach(id => pipeline.hgetall(`user_stats:${id}`));
        accounts = await pipeline.exec();
    }

    res.status(200).json({
      logs: logs,
      accounts: accounts
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
}
