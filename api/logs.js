import { kv } from '@vercel/kv';

export default async function handler(req, res) {
  // CORS configuration
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    'Cache-Control': 'public, max-age=5, stale-while-revalidate=30'
  };

  // Set headers
  Object.entries(corsHeaders).forEach(([key, value]) => {
    res.setHeader(key, value);
  });

  // Handle preflight
  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  // Only GET allowed
  if (req.method !== 'GET') {
    return res.status(405).json({
      error: 'Method not allowed',
      allowed: ['GET', 'OPTIONS']
    });
  }

  try {
    // Parse query parameters dengan defaults
    const {
      limit = '50',
      offset = '0',
      sort = 'recent',
      order = 'desc',
      include_logs = 'true',
      include_stats = 'true',
      format = 'json',
      user_id = null
    } = req.query;

    // Parse numbers dengan validation
    const limitNum = Math.min(Math.max(parseInt(limit, 10) || 50, 1), 200);
    const offsetNum = Math.max(parseInt(offset, 10) || 0, 0);
    
    const results = {
      success: true,
      api_version: '1.2',
      requested_at: new Date().toISOString(),
      query: {
        limit: limitNum,
        offset: offsetNum,
        sort,
        order,
        include_logs: include_logs === 'true',
        include_stats: include_stats === 'true',
        user_id
      }
    };

    // Jika diminta logs
    if (include_logs === 'true') {
      const logs = await kv.lrange('fish_logs', offsetNum, offsetNum + limitNum - 1);
      
      results.logs = {
        count: logs.length,
        total_available: await kv.llen('fish_logs'),
        entries: logs.map((log, index) => {
          try {
            const parsed = JSON.parse(log);
            return {
              id: `${offsetNum + index + 1}`,
              ...parsed,
              _parsed: true
            };
          } catch (e) {
            return {
              id: `${offsetNum + index + 1}`,
              raw: log.substring(0, 100) + (log.length > 100 ? '...' : ''),
              error: 'Parse failed',
              _parsed: false
            };
          }
        })
      };
    }

    // Get user data
    let userIds = [];
    
    if (user_id) {
      // Specific user request
      userIds = [user_id];
    } else {
      // All active users
      userIds = await kv.smembers('active_users');
      results.meta = {
        total_active_users: userIds.length
      };
    }

    // Fetch user stats dengan pipeline untuk performa
    let accounts = [];
    if (userIds.length > 0) {
      const pipeline = kv.pipeline();
      userIds.forEach(id => {
        pipeline.hgetall(`user_stats:${id}`);
      });
      
      const userStats = await pipeline.exec();
      
      accounts = userStats.map((stats, index) => {
        if (!stats || Object.keys(stats).length === 0) {
          return null;
        }

        const userId = userIds[index];
        const totalValue = Number(stats.total_value) || 0;
        const totalFish = Number(stats.total_fish) || 0;
        const lastActive = Number(stats.last_active) || 0;

        return {
          rank: 0, // Akan diisi nanti
          user_id: userId,
          username: stats.username || stats.last_username || 'Unknown',
          stats: {
            total_value: totalValue,
            total_fish: totalFish,
            avg_value: totalFish > 0 ? (totalValue / totalFish).toFixed(2) : 0,
            max_fish_value: Number(stats.max_fish_value) || 0,
            max_fish_name: stats.max_fish_name || 'None',
            last_active: lastActive,
            last_active_human: lastActive ? new Date(lastActive).toISOString() : 'Never',
            rarity_common: Number(stats.rarity_common) || 0,
            rarity_uncommon: Number(stats.rarity_uncommon) || 0,
            rarity_rare: Number(stats.rarity_rare) || 0,
            rarity_epic: Number(stats.rarity_epic) || 0,
            rarity_legendary: Number(stats.rarity_legendary) || 0
          },
          performance: {
            value_per_hour: 0, // Bisa dihitung jika ada data timestamp
            last_7_days: 0,
            streak: 0
          }
        };
      }).filter(account => account !== null);

      // Sorting berdasarkan parameter
      const sortField = sort === 'value' ? 'total_value' : 
                       sort === 'fish' ? 'total_fish' : 'last_active';
      
      accounts.sort((a, b) => {
        const aVal = sortField === 'last_active' ? a.stats[sortField] : a.stats[sortField];
        const bVal = sortField === 'last_active' ? b.stats[sortField] : b.stats[sortField];
        
        if (order === 'desc') {
          return bVal - aVal;
        } else {
          return aVal - bVal;
        }
      });

      // Tambahkan ranking
      accounts.forEach((account, index) => {
        account.rank = order === 'desc' ? index + 1 : accounts.length - index;
      });
    }

    results.accounts = {
      count: accounts.length,
      users: accounts
    };

    // Jika diminta stats global
    if (include_stats === 'true') {
      // Hitung stats dari accounts
      const allValues = accounts.map(a => a.stats.total_value);
      const allFishCounts = accounts.map(a => a.stats.total_fish);
      
      results.global_stats = {
        summary: {
          total_users: accounts.length,
          total_value: accounts.reduce((sum, acc) => sum + acc.stats.total_value, 0),
          total_fish: accounts.reduce((sum, acc) => sum + acc.stats.total_fish, 0),
          avg_value_per_user: accounts.length > 0 ? 
            (accounts.reduce((sum, acc) => sum + acc.stats.total_value, 0) / accounts.length).toFixed(2) : 0,
          avg_fish_per_user: accounts.length > 0 ? 
            (accounts.reduce((sum, acc) => sum + acc.stats.total_fish, 0) / accounts.length).toFixed(2) : 0
        },
        leaderboard: {
          top_by_value: accounts.slice(0, 3).map(acc => ({
            username: acc.username,
            value: acc.stats.total_value
          })),
          top_by_fish: [...accounts]
            .sort((a, b) => b.stats.total_fish - a.stats.total_fish)
            .slice(0, 3)
            .map(acc => ({
              username: acc.username,
              fish_count: acc.stats.total_fish
            }))
        },
        distribution: {
          value_range: {
            min: allValues.length > 0 ? Math.min(...allValues) : 0,
            max: allValues.length > 0 ? Math.max(...allValues) : 0,
            median: allValues.length > 0 ? 
              allValues.sort((a, b) => a - b)[Math.floor(allValues.length / 2)] : 0
          }
        }
      };

      // Coba ambil hourly stats terkini
      try {
        const currentHour = Math.floor(Date.now() / (60 * 60 * 1000));
        const hourKey = `stats:hour:${currentHour}`;
        const hourStats = await kv.hgetall(hourKey);
        
        if (hourStats) {
          results.hourly_stats = {
            hour: currentHour,
            hour_start: new Date(currentHour * 60 * 60 * 1000).toISOString(),
            catches: Number(hourStats.total_catches) || 0,
            value: Number(hourStats.total_value) || 0
          };
        }
      } catch (e) {
        // Skip jika error
      }
    }

    // Response format
    if (format === 'csv') {
      // Simple CSV output (hanya untuk accounts)
      const csvHeaders = ['Rank', 'User ID', 'Username', 'Total Value', 'Total Fish', 'Last Active'];
      const csvRows = accounts.map(acc => [
        acc.rank,
        acc.user_id,
        `"${acc.username.replace(/"/g, '""')}"`,
        acc.stats.total_value,
        acc.stats.total_fish,
        acc.stats.last_active_human
      ].join(','));
      
      res.setHeader('Content-Type', 'text/csv');
      res.setHeader('Content-Disposition', 'attachment; filename="fish_stats.csv"');
      return res.status(200).send([csvHeaders.join(','), ...csvRows].join('\n'));
    }

    return res.status(200).json(results);

  } catch (error) {
    console.error('Logs API Error:', {
      error: error.message,
      stack: error.stack,
      query: req.query,
      timestamp: new Date().toISOString()
    });

    return res.status(500).json({
      success: false,
      error: 'Internal server error',
      code: 'FETCH_ERROR',
      message: process.env.NODE_ENV === 'development' ? error.message : 'Please try again later',
      timestamp: new Date().toISOString()
    });
  }
}
