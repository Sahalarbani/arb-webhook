import { kv } from '@vercel/kv';

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const data = req.body;

  // Validasi sederhana
  if (!data || !data.account) {
    return res.status(400).json({ error: 'Invalid payload' });
  }

  try {
    // 1. Simpan ke Global Logs (List) - Max 100 entry terakhir
    await kv.lpush('fish_logs', JSON.stringify(data));
    await kv.ltrim('fish_logs', 0, 99);

    // 2. Update Stats per User (Hash Map)
    // Key: user_stats:UserId
    const userKey = `user_stats:${data.account.user_id}`;
    
    // Ambil data lama user
    let userStats = await kv.hgetall(userKey) || { 
      username: data.account.username,
      total_value: 0,
      total_fish: 0,
      last_active: 0
    };

    // Update kalkulasi
    userStats.username = data.account.username; // Update nama kalau ganti
    userStats.total_value = Number(userStats.total_value || 0) + (Number(data.fish.price) || 0);
    userStats.total_fish = Number(userStats.total_fish || 0) + 1;
    userStats.last_active = Date.now();

    // Simpan balik stats user
    await kv.hset(userKey, userStats);

    // 3. Masukkan user ke daftar "Active Users" (Set) agar kita tau siapa aja yg main
    await kv.sadd('active_users', data.account.user_id);

    return res.status(200).json({ success: true });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ error: 'Database Error' });
  }
}
