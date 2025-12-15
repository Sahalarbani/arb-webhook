import { kv } from '@vercel/kv';

export default async function handler(req, res) {
  // CORS headers untuk development
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  // Handle preflight
  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  // Hanya terima POST
  if (req.method !== 'POST') {
    res.setHeader('Allow', ['POST']);
    return res.status(405).json({
      error: 'Method not allowed',
      allowed: ['POST']
    });
  }

  // Rate limiting key (opsional)
  const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
  const rateKey = `rate_limit:${ip}`;
  const rateCount = await kv.get(rateKey) || 0;

  if (rateCount > 100) { // Max 100 requests per hour per IP
    return res.status(429).json({
      error: 'Too many requests',
      retry_after: '1 hour'
    });
  }

  try {
    const data = req.body;

    // Validasi payload
    if (!data || typeof data !== 'object') {
      return res.status(400).json({ error: 'Invalid payload format' });
    }

    if (!data.account || !data.account.user_id || !data.account.username) {
      return res.status(400).json({ 
        error: 'Missing account information',
        required: ['account.user_id', 'account.username']
      });
    }

    // Data ikan default jika tidak ada
    const fishData = {
      name: data.fish?.name || 'Unknown Fish',
      price: Number(data.fish?.price) || 0,
      rarity: data.fish?.rarity || 'common',
      weight: Number(data.fish?.weight) || 0
    };

    const timestamp = Date.now();
    const userId = String(data.account.user_id);
    
    // Prepare log entry dengan metadata
    const logEntry = {
      event: 'fish_caught',
      user_id: userId,
      username: data.account.username,
      fish: fishData,
      location: data.location || 'unknown',
      timestamp: timestamp,
      iso_time: new Date(timestamp).toISOString(),
      session_id: data.session_id || null
    };

    // Atomic operations pipeline
    const pipeline = kv.pipeline();

    // 1. Save to logs (max 200 entries)
    pipeline.lpush('fish_logs', JSON.stringify(logEntry));
    pipeline.ltrim('fish_logs', 0, 199);

    // 2. Update user stats dengan operasi atomic
    const userKey = `user_stats:${userId}`;
    
    // Update hash fields
    pipeline.hset(userKey, {
      username: data.account.username,
      user_id: userId,
      last_username: data.account.username,
      last_active: timestamp,
      last_fish: fishData.name,
      last_fish_rarity: fishData.rarity
    });
    
    // Increment counters
    pipeline.hincrbyfloat(userKey, 'total_value', fishData.price);
    pipeline.hincrby(userKey, 'total_fish', 1);
    
    // Increment rarity counters
    pipeline.hincrby(userKey, `rarity_${fishData.rarity}`, 1);
    
    // Update max fish value jika lebih besar
    pipeline.hget(userKey, 'max_fish_value').then((maxValue) => {
      const currentMax = Number(maxValue) || 0;
      if (fishData.price > currentMax) {
        pipeline.hset(userKey, 'max_fish_value', fishData.price);
        pipeline.hset(userKey, 'max_fish_name', fishData.name);
      }
    });

    // 3. Active users tracking
    pipeline.sadd('active_users', userId);
    
    // 4. Hourly statistics
    const hourBucket = Math.floor(timestamp / (60 * 60 * 1000));
    const hourKey = `stats:hour:${hourBucket}`;
    pipeline.hincrby(hourKey, 'total_catches', 1);
    pipeline.hincrbyfloat(hourKey, 'total_value', fishData.price);
    pipeline.expire(hourKey, 48 * 60 * 60); // 48 hours expiry

    // 5. Rarity leaderboard
    const rarityLeaderboard = `leaderboard:rarity:${fishData.rarity}`;
    pipeline.zincrby(rarityLeaderboard, 1, userId);

    // 6. Value leaderboard
    const valueLeaderboard = 'leaderboard:value';
    pipeline.zincrby(valueLeaderboard, fishData.price, userId);

    // 7. Rate limiting
    pipeline.setex(rateKey, 3600, Number(rateCount) + 1);

    // Execute all operations
    await pipeline.exec();

    // Response sukses
    return res.status(200).json({
      success: true,
      message: 'Fish catch recorded successfully',
      data: {
        user_id: userId,
        username: data.account.username,
        fish: fishData,
        timestamp: timestamp,
        iso_time: new Date(timestamp).toISOString(),
        total_value: fishData.price
      },
      metadata: {
        version: '1.0',
        processed_at: Date.now()
      }
    });

  } catch (error) {
    console.error('Webhook Error:', {
      error: error.message,
      stack: error.stack,
      timestamp: new Date().toISOString(),
      body: req.body
    });

    // Error response berdasarkan tipe error
    if (error.message.includes('Redis') || error.message.includes('KV')) {
      return res.status(503).json({
        error: 'Database service temporarily unavailable',
        code: 'DB_UNAVAILABLE',
        suggestion: 'Retry in a few moments'
      });
    }

    return res.status(500).json({
      error: 'Internal server error',
      code: 'INTERNAL_ERROR',
      request_id: Date.now().toString(36) + Math.random().toString(36).substr(2)
    });
  }
}
