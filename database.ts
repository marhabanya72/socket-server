// /Users/macbook/Documents/n1verse/src/lib/database.ts
import mysql from 'mysql2/promise';
import crypto from 'crypto';

const pool = mysql.createPool({
  host: process.env.DB_HOST?.split(':')[0] || 'db-fde-02.sparkedhost.us',
  port: parseInt(process.env.DB_HOST?.split(':')[1] || '3306'),
  user: process.env.DB_USER || 'u175260_2aWtznM6FW',
  password: process.env.DB_PASSWORD || 'giqaKuZnR72ZdQL=m.DVdtUB',
  database: process.env.DB_NAME || 's175260_casino-n1verse',
  waitForConnections: true,
});

// Points and Experience System Logic
export class PointsSystem {
  // 1000 points per 100 USD wagered = 10 points per USD
  static calculatePointsFromWager(wagerAmount: number): number {
    return Math.floor(wagerAmount * 10);
  }

  // Experience levels: Level N requires N * 800 XP total
  static calculateLevelFromExperience(experience: number): number {
    return Math.floor(experience / 800) + 1;
  }

  static getExperienceForLevel(level: number): number {
    return (level - 1) * 800;
  }

  static getExperienceNeededForNextLevel(currentExp: number): { currentLevel: number, expForCurrentLevel: number, expForNextLevel: number, progress: number } {
    const currentLevel = this.calculateLevelFromExperience(currentExp);
    const expForCurrentLevel = this.getExperienceForLevel(currentLevel);
    const expForNextLevel = this.getExperienceForLevel(currentLevel + 1);
    const progress = currentExp - expForCurrentLevel;
    
    return {
      currentLevel,
      expForCurrentLevel,
      expForNextLevel,
      progress
    };
  }
}

// Initialize database tables
export async function initializeDatabase() {
  try {
    const connection = await pool.getConnection();

    // Users table
    await connection.execute(`
      CREATE TABLE IF NOT EXISTS users (
        id VARCHAR(36) PRIMARY KEY,
        username VARCHAR(50) UNIQUE NOT NULL,
        email VARCHAR(255),
        wallet_address VARCHAR(255) UNIQUE NOT NULL,
        balance DECIMAL(20, 8) DEFAULT 1000.00000000,
        profile_picture TEXT,
        referral_code VARCHAR(20),
        total_wagered DECIMAL(20, 8) DEFAULT 0.00000000,
        total_won DECIMAL(20, 8) DEFAULT 0.00000000,
        games_played INT DEFAULT 0,
        points BIGINT DEFAULT 0,
        experience INT DEFAULT 0,
        level INT DEFAULT 1,
        beta_code_id VARCHAR(36) DEFAULT NULL,
        has_beta_access TINYINT(1) DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        is_active BOOLEAN DEFAULT TRUE,
        INDEX idx_wallet (wallet_address),
        INDEX idx_username (username),
        INDEX idx_created_at (created_at),
        INDEX idx_points (points),
        INDEX idx_level (level),
        INDEX idx_experience (experience),
        INDEX idx_beta_code_id (beta_code_id),
        CONSTRAINT users_beta_code_fk FOREIGN KEY (beta_code_id) REFERENCES beta_codes (id) ON DELETE SET NULL
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);

    // Beta codes table
    await connection.execute(`
      CREATE TABLE IF NOT EXISTS beta_codes (
        id VARCHAR(36) NOT NULL PRIMARY KEY,
        code VARCHAR(6) NOT NULL UNIQUE,
        is_used TINYINT(1) DEFAULT 0,
        used_by VARCHAR(36) DEFAULT NULL,
        used_at TIMESTAMP NULL DEFAULT NULL,
        created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
        expires_at TIMESTAMP NULL DEFAULT NULL,
        KEY idx_is_used (is_used),
        KEY idx_used_by (used_by),
        CONSTRAINT beta_codes_ibfk_1 FOREIGN KEY (used_by) REFERENCES users (id) ON DELETE SET NULL
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);

    // Check if beta_code_id column exists in users table, add if not
    const [columns] = await connection.execute(
      `SHOW COLUMNS FROM users WHERE Field = 'beta_code_id'`
    );
    
    if ((columns as any[]).length === 0) {
      // Add beta_code_id and has_beta_access columns to users table
      await connection.execute(`
        ALTER TABLE users
        ADD COLUMN beta_code_id VARCHAR(36) DEFAULT NULL AFTER level,
        ADD COLUMN has_beta_access TINYINT(1) DEFAULT 0 AFTER beta_code_id,
        ADD KEY idx_beta_code_id (beta_code_id),
        ADD CONSTRAINT users_beta_code_fk FOREIGN KEY (beta_code_id) REFERENCES beta_codes (id) ON DELETE SET NULL
      `);
    }

    // Dice games table
    await connection.execute(`
      CREATE TABLE IF NOT EXISTS dice_games (
        id VARCHAR(100) PRIMARY KEY,
        server_seed VARCHAR(255) NOT NULL,
        hashed_seed VARCHAR(255) NOT NULL,
        public_seed VARCHAR(255),
        nonce INT NOT NULL,
        dice_value INT,
        is_odd BOOLEAN,
        total_wagered DECIMAL(20, 8) DEFAULT 0.00000000,
        total_payout DECIMAL(20, 8) DEFAULT 0.00000000,
        players_count INT DEFAULT 0,
        status ENUM('betting', 'rolling', 'complete') DEFAULT 'betting',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        completed_at TIMESTAMP NULL,
        INDEX idx_status (status),
        INDEX idx_created_at (created_at)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);

    // Dice bets table
    await connection.execute(`
      CREATE TABLE IF NOT EXISTS dice_bets (
        id VARCHAR(36) PRIMARY KEY,
        game_id VARCHAR(100) NOT NULL,
        user_id VARCHAR(36) NOT NULL,
        amount DECIMAL(20, 8) NOT NULL,
        choice ENUM('odd', 'even') NOT NULL,
        payout DECIMAL(20, 8) DEFAULT 0.00000000,
        is_winner BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (game_id) REFERENCES dice_games(id) ON DELETE CASCADE,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
        INDEX idx_game_id (game_id),
        INDEX idx_user_id (user_id),
        INDEX idx_created_at (created_at)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);

    // Crash games table
    await connection.execute(`
      CREATE TABLE IF NOT EXISTS crash_games (
        id VARCHAR(100) PRIMARY KEY,
        server_seed VARCHAR(255) NOT NULL,
        hashed_seed VARCHAR(255) NOT NULL,
        public_seed VARCHAR(255),
        nonce INT NOT NULL,
        crash_point DECIMAL(10, 2),
        total_wagered DECIMAL(20, 8) DEFAULT 0.00000000,
        total_payout DECIMAL(20, 8) DEFAULT 0.00000000,
        players_count INT DEFAULT 0,
        status ENUM('betting', 'flying', 'crashed', 'complete') DEFAULT 'betting',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        crashed_at TIMESTAMP NULL,
        INDEX idx_status (status),
        INDEX idx_created_at (created_at)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);

    // Crash bets table
    await connection.execute(`
      CREATE TABLE IF NOT EXISTS crash_bets (
        id VARCHAR(36) PRIMARY KEY,
        game_id VARCHAR(100) NOT NULL,
        user_id VARCHAR(36) NOT NULL,
        amount DECIMAL(20, 8) NOT NULL,
        cash_out_at DECIMAL(10, 2) DEFAULT NULL,
        payout DECIMAL(20, 8) DEFAULT 0.00000000,
        is_cashed_out BOOLEAN DEFAULT FALSE,
        is_winner BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        cashed_out_at TIMESTAMP NULL,
        FOREIGN KEY (game_id) REFERENCES crash_games(id) ON DELETE CASCADE,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
        INDEX idx_game_id (game_id),
        INDEX idx_user_id (user_id),
        INDEX idx_created_at (created_at)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);

    // RPS lobbies table
    await connection.execute(`
      CREATE TABLE IF NOT EXISTS rps_lobbies (
        id VARCHAR(100) PRIMARY KEY,
        creator_id VARCHAR(36) NOT NULL,
        opponent_id VARCHAR(36),
        amount DECIMAL(20, 8) NOT NULL,
        status ENUM('waiting', 'ready', 'in-progress', 'vs-bot', 'completed') DEFAULT 'waiting',
        hashed_seed VARCHAR(255) NOT NULL,
        server_seed VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        timeout_at TIMESTAMP,
        FOREIGN KEY (creator_id) REFERENCES users(id) ON DELETE CASCADE,
        FOREIGN KEY (opponent_id) REFERENCES users(id) ON DELETE CASCADE,
        INDEX idx_status (status),
        INDEX idx_creator_id (creator_id),
        INDEX idx_created_at (created_at)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);

    // RPS battles table
    await connection.execute(`
      CREATE TABLE IF NOT EXISTS rps_battles (
        id VARCHAR(100) PRIMARY KEY,
        lobby_id VARCHAR(100) NOT NULL,
        player1_id VARCHAR(36) NOT NULL,
        player2_id VARCHAR(36),
        player1_move ENUM('rock', 'paper', 'scissors'),
        player2_move ENUM('rock', 'paper', 'scissors'),
        winner_id VARCHAR(36),
        amount DECIMAL(20, 8) NOT NULL,
        payout DECIMAL(20, 8) DEFAULT 0.00000000,
        server_seed VARCHAR(255) NOT NULL,
        hashed_seed VARCHAR(255) NOT NULL,
        nonce INT NOT NULL,
        is_vs_bot BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (lobby_id) REFERENCES rps_lobbies(id) ON DELETE CASCADE,
        FOREIGN KEY (player1_id) REFERENCES users(id) ON DELETE CASCADE,
        FOREIGN KEY (player2_id) REFERENCES users(id) ON DELETE CASCADE,
        FOREIGN KEY (winner_id) REFERENCES users(id) ON DELETE SET NULL,
        INDEX idx_lobby_id (lobby_id),
        INDEX idx_player1_id (player1_id),
        INDEX idx_created_at (created_at)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);

    // Chat messages table
    await connection.execute(`
      CREATE TABLE IF NOT EXISTS chat_messages (
        id VARCHAR(36) PRIMARY KEY,
        user_id VARCHAR(36) NOT NULL,
        message TEXT NOT NULL,
        is_system_message BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
        INDEX idx_user_id (user_id),
        INDEX idx_created_at (created_at)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);

    // Game statistics table
    await connection.execute(`
      CREATE TABLE IF NOT EXISTS game_statistics (
        id INT AUTO_INCREMENT PRIMARY KEY,
        date DATE NOT NULL,
        game_type ENUM('dice', 'rps', 'crash') NOT NULL,
        total_games INT DEFAULT 0,
        total_wagered DECIMAL(20, 8) DEFAULT 0.00000000,
        total_payout DECIMAL(20, 8) DEFAULT 0.00000000,
        unique_players INT DEFAULT 0,
        house_profit DECIMAL(20, 8) DEFAULT 0.00000000,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY unique_date_game (date, game_type),
        INDEX idx_date (date),
        INDEX idx_game_type (game_type)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);

    // RPS user history table (personal battle history for each user)
    await connection.execute(`
      CREATE TABLE IF NOT EXISTS rps_user_history (
        id VARCHAR(100) PRIMARY KEY,
        user_id VARCHAR(36) NOT NULL,
        opponent_id VARCHAR(36),
        opponent_username VARCHAR(50),
        user_move ENUM('rock', 'paper', 'scissors') NOT NULL,
        opponent_move ENUM('rock', 'paper', 'scissors') NOT NULL,
        result ENUM('win', 'lose', 'draw') NOT NULL,
        amount DECIMAL(20, 8) NOT NULL,
        payout DECIMAL(20, 8) DEFAULT 0.00000000,
        is_vs_bot BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
        FOREIGN KEY (opponent_id) REFERENCES users(id) ON DELETE SET NULL,
        INDEX idx_user_id (user_id),
        INDEX idx_created_at (created_at),
        INDEX idx_result (result)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);

    // RPS recent battles table (public recent battles visible to all users)
    await connection.execute(`
      CREATE TABLE IF NOT EXISTS rps_recent_battles (
        id VARCHAR(100) PRIMARY KEY,
        player1_id VARCHAR(36) NOT NULL,
        player1_username VARCHAR(50) NOT NULL,
        player1_avatar TEXT,
        player1_move ENUM('rock', 'paper', 'scissors') NOT NULL,
        player2_id VARCHAR(36),
        player2_username VARCHAR(50),
        player2_avatar TEXT,
        player2_move ENUM('rock', 'paper', 'scissors') NOT NULL,
        winner_id VARCHAR(36),
        winner_username VARCHAR(50),
        amount DECIMAL(20, 8) NOT NULL,
        payout DECIMAL(20, 8) DEFAULT 0.00000000,
        is_vs_bot BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (player1_id) REFERENCES users(id) ON DELETE CASCADE,
        FOREIGN KEY (player2_id) REFERENCES users(id) ON DELETE SET NULL,
        FOREIGN KEY (winner_id) REFERENCES users(id) ON DELETE SET NULL,
        INDEX idx_created_at (created_at),
        INDEX idx_player1_id (player1_id),
        INDEX idx_player2_id (player2_id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);

    connection.release();
    console.log('Database tables initialized successfully');
    
  } catch (error) {
    console.error('Error initializing database:', error);
    throw error;
  }
}

// Beta Code Database Operations
export class BetaCodeDatabase {
  static async getCodeByValue(code: string) {
    const connection = await pool.getConnection();
    try {
      const [codes] = await connection.execute(
        'SELECT * FROM beta_codes WHERE code = ?',
        [code]
      );
      return (codes as any[])[0] || null;
    } finally {
      connection.release();
    }
  }

  static async getCodeById(codeId: string) {
    const connection = await pool.getConnection();
    try {
      const [codes] = await connection.execute(
        'SELECT * FROM beta_codes WHERE id = ?',
        [codeId]
      );
      return (codes as any[])[0] || null;
    } finally {
      connection.release();
    }
  }

  static async markCodeAsUsed(codeId: string, userId: string) {
    const connection = await pool.getConnection();
    try {
      await connection.execute(
        'UPDATE beta_codes SET is_used = 1, used_by = ?, used_at = NOW() WHERE id = ?',
        [userId, codeId]
      );
    } finally {
      connection.release();
    }
  }

  static async createCode(code: string, expiresAt?: Date) {
    const connection = await pool.getConnection();
    try {
      const id = 'bc_' + crypto.randomBytes(16).toString('hex');
      await connection.execute(
        'INSERT INTO beta_codes (id, code, expires_at) VALUES (?, ?, ?)',
        [id, code, expiresAt || null]
      );
      return id;
    } finally {
      connection.release();
    }
  }

  static async getUnusedCodes(limit: number = 100) {
    const connection = await pool.getConnection();
    try {
      const [codes] = await connection.execute(
        `SELECT * FROM beta_codes 
         WHERE is_used = 0 AND (expires_at IS NULL OR expires_at > NOW())
         ORDER BY created_at DESC
         LIMIT ?`,
        [limit]
      );
      return codes as any[];
    } finally {
      connection.release();
    }
  }

  static async getUsedCodes(limit: number = 100) {
    const connection = await pool.getConnection();
    try {
      const [codes] = await connection.execute(
        `SELECT bc.*, u.username 
         FROM beta_codes bc
         LEFT JOIN users u ON bc.used_by = u.id
         WHERE bc.is_used = 1
         ORDER BY bc.used_at DESC
         LIMIT ?`,
        [limit]
      );
      return codes as any[];
    } finally {
      connection.release();
    }
  }

  static async getCodeStats() {
    const connection = await pool.getConnection();
    try {
      const [stats] = await connection.execute(`
        SELECT 
          COUNT(*) as total_codes,
          SUM(is_used) as used_codes,
          SUM(CASE WHEN is_used = 0 THEN 1 ELSE 0 END) as unused_codes,
          SUM(CASE WHEN expires_at < NOW() THEN 1 ELSE 0 END) as expired_codes
        FROM beta_codes
      `);
      return (stats as any[])[0];
    } finally {
      connection.release();
    }
  }
}

// User operations
export class UserDatabase {
  static async createUser(userData: {
    id: string;
    username: string;
    email?: string;
    walletAddress: string;
    referralCode?: string;
    profilePicture: string;
    betaCodeId?: string;
    hasBetaAccess?: boolean;
  }) {
    const connection = await pool.getConnection();
    try {
      await connection.execute(
        `INSERT INTO users (id, username, email, wallet_address, referral_code, profile_picture, points, experience, level, beta_code_id, has_beta_access)
         VALUES (?, ?, ?, ?, ?, ?, 0, 0, 1, ?, ?)`,
        [
          userData.id, 
          userData.username, 
          userData.email || null, 
          userData.walletAddress, 
          userData.referralCode || null, 
          userData.profilePicture,
          userData.betaCodeId || null,
          userData.hasBetaAccess ? 1 : 0
        ]
      );
      
      return await this.getUserByWallet(userData.walletAddress);
    } finally {
      connection.release();
    }
  }

  static async getUserByWallet(walletAddress: string) {
    const connection = await pool.getConnection();
    try {
      const [rows] = await connection.execute(
        'SELECT * FROM users WHERE wallet_address = ?',
        [walletAddress]
      );
      return (rows as any[])[0] || null;
    } finally {
      connection.release();
    }
  }

  static async getUserById(userId: string) {
    const connection = await pool.getConnection();
    try {
      const [rows] = await connection.execute(
        'SELECT * FROM users WHERE id = ?',
        [userId]
      );
      return (rows as any[])[0] || null;
    } finally {
      connection.release();
    }
  }

  static async getUserByUsername(username: string) {
    const connection = await pool.getConnection();
    try {
      const [rows] = await connection.execute(
        'SELECT * FROM users WHERE username = ?',
        [username]
      );
      return (rows as any[])[0] || null;
    } finally {
      connection.release();
    }
  }

  static async updateUserBalance(userId: string, amount: number, operation: 'add' | 'subtract' | 'set' = 'add') {
    const connection = await pool.getConnection();
    try {
      let query: string;
      
      if (operation === 'set') {
        query = 'UPDATE users SET balance = ?, last_active = CURRENT_TIMESTAMP WHERE id = ?';
        await connection.execute(query, [amount, userId]);
      } else if (operation === 'add') {
        query = 'UPDATE users SET balance = balance + ?, last_active = CURRENT_TIMESTAMP WHERE id = ?';
        await connection.execute(query, [amount, userId]);
      } else {
        query = 'UPDATE users SET balance = balance - ?, last_active = CURRENT_TIMESTAMP WHERE id = ? AND balance >= ?';
        const [result] = await connection.execute(query, [amount, userId, amount]);
        if ((result as any).affectedRows === 0) {
          throw new Error('Insufficient balance');
        }
      }
      
      // Return updated user
      return await this.getUserById(userId);
    } finally {
      connection.release();
    }
  }

  static async updateUserStats(userId: string, wagered: number, won: number, gamesPlayed: number = 1) {
    const connection = await pool.getConnection();
    try {
      // Calculate points from wager (10 points per USD wagered)
      const pointsEarned = PointsSystem.calculatePointsFromWager(wagered);
      const experienceGained = Math.floor(wagered); // 1 XP per USD wagered
      
      await connection.execute(
        `UPDATE users SET 
         total_wagered = total_wagered + ?,
         total_won = total_won + ?,
         games_played = games_played + ?,
         points = points + ?,
         experience = experience + ?,
         last_active = CURRENT_TIMESTAMP
         WHERE id = ?`,
        [wagered, won, gamesPlayed, pointsEarned, experienceGained, userId]
      );

      // Update level based on new experience
      const user = await this.getUserById(userId);
      if (user) {
        const newLevel = PointsSystem.calculateLevelFromExperience(user.experience);
        if (newLevel !== user.level) {
          await connection.execute(
            'UPDATE users SET level = ? WHERE id = ?',
            [newLevel, userId]
          );
        }
      }
    } finally {
      connection.release();
    }
  }

  static async updateUserPoints(userId: string, points: number, experience: number) {
    const connection = await pool.getConnection();
    try {
      await connection.execute(
        `UPDATE users 
         SET points = points + ?, 
             experience = experience + ?,
             level = FLOOR(experience / 800) + 1,
             last_active = CURRENT_TIMESTAMP
         WHERE id = ?`,
        [points, experience, userId]
      );
    } finally {
      connection.release();
    }
  }

  static async updateProfilePicture(userId: string, profilePicture: string) {
    const connection = await pool.getConnection();
    try {
      await connection.execute(
        'UPDATE users SET profile_picture = ?, last_active = CURRENT_TIMESTAMP WHERE id = ?',
        [profilePicture, userId]
      );
      return await this.getUserById(userId);
    } finally {
      connection.release();
    }
  }

  static async getActiveUsers(limit: number = 10) {
    const connection = await pool.getConnection();
    try {
      const [users] = await connection.execute(
        `SELECT id, username, wallet_address, profile_picture, balance, 
                total_wagered, total_won, games_played, points, level, has_beta_access
         FROM users 
         WHERE is_active = 1 AND has_beta_access = 1
         ORDER BY last_active DESC 
         LIMIT ?`,
        [limit]
      );
      return users as any[];
    } finally {
      connection.release();
    }
  }

  static async getLeaderboard(limit: number = 100) {
    const connection = await pool.getConnection();
    try {
      const [rows] = await connection.execute(
        `SELECT 
          id, username, profile_picture, points, experience, level, 
          total_wagered, total_won, games_played, created_at, has_beta_access,
          ROW_NUMBER() OVER (ORDER BY points DESC, total_wagered DESC) as rank_position
         FROM users 
         WHERE is_active = TRUE AND has_beta_access = 1
         ORDER BY points DESC, total_wagered DESC 
         LIMIT ?`,
        [limit]
      );
      return rows as any[];
    } finally {
      connection.release();
    }
  }

  static async getUserRank(userId: string) {
    const connection = await pool.getConnection();
    try {
      const [rows] = await connection.execute(
        `SELECT rank_position FROM (
          SELECT id, ROW_NUMBER() OVER (ORDER BY points DESC, total_wagered DESC) as rank_position
          FROM users WHERE is_active = TRUE AND has_beta_access = 1
        ) ranked WHERE id = ?`,
        [userId]
      );
      return (rows as any[])[0]?.rank_position || null;
    } finally {
      connection.release();
    }
  }

  static async getUserStats(userId: string) {
    const connection = await pool.getConnection();
    try {
      const user = await this.getUserById(userId);
      if (!user) return null;

      const rank = await this.getUserRank(userId);
      const levelInfo = PointsSystem.getExperienceNeededForNextLevel(user.experience);

      return {
        ...user,
        rank,
        levelInfo
      };
    } finally {
      connection.release();
    }
  }

  static async getAllUsers(limit: number = 100) {
    const connection = await pool.getConnection();
    try {
      const [rows] = await connection.execute(
        'SELECT * FROM users ORDER BY created_at DESC LIMIT ?',
        [limit]
      );
      return rows as any[];
    } finally {
      connection.release();
    }
  }
}

// Dice game operations
export class DiceDatabase {
  static async createGame(gameData: {
    id: string;
    serverSeed: string;
    hashedSeed: string;
    publicSeed?: string;
    nonce: number;
  }) {
    const connection = await pool.getConnection();
    try {
      await connection.execute(
        `INSERT INTO dice_games (id, server_seed, hashed_seed, public_seed, nonce)
         VALUES (?, ?, ?, ?, ?)`,
        [gameData.id, gameData.serverSeed, gameData.hashedSeed, gameData.publicSeed || null, gameData.nonce]
      );
    } finally {
      connection.release();
    }
  }

  static async completeGame(gameId: string, result: {
    diceValue: number;
    isOdd: boolean;
    totalWagered: number;
    totalPayout: number;
    playersCount: number;
  }) {
    const connection = await pool.getConnection();
    try {
      await connection.execute(
        `UPDATE dice_games SET 
         dice_value = ?, is_odd = ?, total_wagered = ?, total_payout = ?, 
         players_count = ?, status = 'complete', completed_at = CURRENT_TIMESTAMP
         WHERE id = ?`,
        [result.diceValue, result.isOdd, result.totalWagered, result.totalPayout, result.playersCount, gameId]
      );
    } finally {
      connection.release();
    }
  }

  static async placeBet(betData: {
    id: string;
    gameId: string;
    userId: string;
    amount: number;
    choice: 'odd' | 'even';
  }) {
    const connection = await pool.getConnection();
    try {
      await connection.execute(
        `INSERT INTO dice_bets (id, game_id, user_id, amount, choice)
         VALUES (?, ?, ?, ?, ?)`,
        [betData.id, betData.gameId, betData.userId, betData.amount, betData.choice]
      );
    } finally {
      connection.release();
    }
  }

  static async updateBetResult(betId: string, isWinner: boolean, payout: number) {
    const connection = await pool.getConnection();
    try {
      await connection.execute(
        'UPDATE dice_bets SET is_winner = ?, payout = ? WHERE id = ?',
        [isWinner, payout, betId]
      );
    } finally {
      connection.release();
    }
  }

  static async getGameHistory(limit: number = 20) {
    const connection = await pool.getConnection();
    try {
      const [rows] = await connection.execute(
        `SELECT dg.*, COUNT(db.id) as bet_count, COALESCE(SUM(db.amount), 0) as total_wagered
         FROM dice_games dg
         LEFT JOIN dice_bets db ON dg.id = db.game_id
         WHERE dg.status = 'complete'
         GROUP BY dg.id
         ORDER BY dg.completed_at DESC
         LIMIT ?`,
        [limit]
      );
      return rows as any[];
    } finally {
      connection.release();
    }
  }
}

// Crash game operations
export class CrashDatabase {
  static async createGame(gameData: {
    id: string;
    serverSeed: string;
    hashedSeed: string;
    publicSeed?: string;
    nonce: number;
  }) {
    const connection = await pool.getConnection();
    try {
      await connection.execute(
        `INSERT INTO crash_games (id, server_seed, hashed_seed, public_seed, nonce)
         VALUES (?, ?, ?, ?, ?)`,
        [gameData.id, gameData.serverSeed, gameData.hashedSeed, gameData.publicSeed || null, gameData.nonce]
      );
    } finally {
      connection.release();
    }
  }

  static async completeGame(gameId: string, result: {
    crashPoint: number;
    totalWagered: number;
    totalPayout: number;
    playersCount: number;
  }) {
    const connection = await pool.getConnection();
    try {
      await connection.execute(
        `UPDATE crash_games SET 
         crash_point = ?, total_wagered = ?, total_payout = ?, 
         players_count = ?, status = 'complete', crashed_at = CURRENT_TIMESTAMP
         WHERE id = ?`,
        [result.crashPoint, result.totalWagered, result.totalPayout, result.playersCount, gameId]
      );
    } finally {
      connection.release();
    }
  }

  static async placeBet(betData: {
    id: string;
    gameId: string;
    userId: string;
    amount: number;
  }) {
    const connection = await pool.getConnection();
    try {
      await connection.execute(
        `INSERT INTO crash_bets (id, game_id, user_id, amount)
         VALUES (?, ?, ?, ?)`,
        [betData.id, betData.gameId, betData.userId, betData.amount]
      );
    } finally {
      connection.release();
    }
  }

  static async getGameDetails(gameId: string) {
  const connection = await pool.getConnection();
  try {
    // Get game details
    const [gameRows] = await connection.execute(
      `SELECT 
        id,
        crash_point,
        total_wagered,
        total_payout,
        players_count,
        hashed_seed,
        server_seed,
        created_at,
        crashed_at,
        status
      FROM crash_games 
      WHERE id = ?`,
      [gameId]
    );

    if (!Array.isArray(gameRows) || gameRows.length === 0) {
      // If no game found in crash_games, try to reconstruct from crash_bets
      const [betCheckRows] = await connection.execute(
        `SELECT COUNT(*) as bet_count FROM crash_bets WHERE game_id = ?`,
        [gameId]
      );
      
      const betCount = (betCheckRows as any[])[0]?.bet_count || 0;
      
      if (betCount === 0) {
        return null; // No game found
      }
      
      // Reconstruct game data from bets
      const [betStatsRows] = await connection.execute(
        `SELECT 
          COUNT(*) as players_count,
          SUM(amount) as total_wagered,
          SUM(payout) as total_payout,
          MAX(CASE WHEN cash_out_at IS NOT NULL THEN cash_out_at ELSE 1.00 END) as estimated_crash_point,
          MIN(created_at) as game_start
        FROM crash_bets 
        WHERE game_id = ?`,
        [gameId]
      );
      
      const stats = (betStatsRows as any[])[0];
      
      return {
        game: {
          id: gameId,
          crash_point: parseFloat(stats.estimated_crash_point || 1.33),
          total_wagered: parseFloat(stats.total_wagered || 0),
          total_payout: parseFloat(stats.total_payout || 0),
          players_count: parseInt(stats.players_count || 0),
          hashed_seed: 'Reconstructed from bets',
          server_seed: null,
          created_at: stats.game_start,
          crashed_at: stats.game_start,
          status: 'complete'
        },
        players: await this.getGamePlayers(gameId)
      };
    }

    const game = gameRows[0] as any;
    const players = await this.getGamePlayers(gameId);

    return {
      game,
      players
    };

  } catch (error) {
    console.error('Error getting crash game details:', error);
    throw error;
  } finally {
    connection.release();
  }
}

static async getGamePlayers(gameId: string) {
  const connection = await pool.getConnection();
  try {
    const [betRows] = await connection.execute(
      `SELECT 
        cb.amount,
        cb.cash_out_at,
        cb.payout,
        cb.is_cashed_out,
        cb.is_winner,
        cb.created_at,
        u.username,
        u.profile_picture
      FROM crash_bets cb
      JOIN users u ON cb.user_id = u.id
      WHERE cb.game_id = ?
      ORDER BY cb.payout DESC, cb.amount DESC`,
      [gameId]
    );

    return (betRows as any[]).map(bet => ({
      username: bet.username,
      amount: parseFloat(bet.amount),
      cashOutAt: bet.cash_out_at ? parseFloat(bet.cash_out_at) : null,
      payout: parseFloat(bet.payout),
      isWinner: bet.is_winner === 1,
      profilePicture: bet.profile_picture
    }));

  } catch (error) {
    console.error('Error getting crash game players:', error);
    throw error;
  } finally {
    connection.release();
  }
}

  static async cashOut(betId: string, cashOutMultiplier: number, payout: number) {
    const connection = await pool.getConnection();
    try {
      await connection.execute(
        `UPDATE crash_bets SET 
         is_cashed_out = TRUE, 
         is_winner = TRUE, 
         cash_out_at = ?, 
         payout = ?, 
         cashed_out_at = CURRENT_TIMESTAMP 
         WHERE id = ?`,
        [cashOutMultiplier, payout, betId]
      );
    } finally {
      connection.release();
    }
  }

  static async updateBetResult(betId: string, isWinner: boolean, payout: number) {
    const connection = await pool.getConnection();
    try {
      await connection.execute(
        'UPDATE crash_bets SET is_winner = ?, payout = ? WHERE id = ?',
        [isWinner, payout, betId]
      );
    } finally {
      connection.release();
    }
  }

  static async getGameHistory(limit: number = 20) {
  const connection = await pool.getConnection();
  try {
    const [rows] = await connection.execute(
      `SELECT cg.*, COUNT(cb.id) as bet_count, COALESCE(SUM(cb.amount), 0) as total_wagered
       FROM crash_games cg
       LEFT JOIN crash_bets cb ON cg.id = cb.game_id
       WHERE cg.status = 'complete'
       GROUP BY cg.id
       ORDER BY cg.crashed_at DESC, cg.created_at DESC, cg.id DESC
       LIMIT ?`,
      [limit]
    );
    
    // Cast rows to array first
    const gameRows = rows as any[];
    
    // Log the retrieved games for debugging
    console.log(`🚀 Database: Retrieved ${gameRows.length} crash games from history`);
    if (gameRows.length > 0) {
      console.log(`🚀 First game: ${gameRows[0].id}, Last game: ${gameRows[gameRows.length - 1].id}`);
    }
    
    return gameRows;
  } finally {
    connection.release();
  }
}

  static async getUserHistory(userId: string, limit: number = 20) {
    const connection = await pool.getConnection();
    try {
      const [rows] = await connection.execute(
        `SELECT cb.*, cg.crash_point, cg.crashed_at
         FROM crash_bets cb
         INNER JOIN crash_games cg ON cb.game_id = cg.id
         WHERE cb.user_id = ? AND cg.status = 'complete'
         ORDER BY cb.created_at DESC
         LIMIT ?`,
        [userId, limit]
      );
      return rows as any[];
    } finally {
      connection.release();
    }
  }
}

// RPS game operations
export class RPSDatabase {
  static async createLobby(lobbyData: {
    id: string;
    creatorId: string;
    amount: number;
    hashedSeed: string;
  }) {
    const connection = await pool.getConnection();
    try {
      await connection.execute(
        `INSERT INTO rps_lobbies (id, creator_id, amount, hashed_seed, timeout_at)
         VALUES (?, ?, ?, ?, DATE_ADD(NOW(), INTERVAL 30 SECOND))`,
        [lobbyData.id, lobbyData.creatorId, lobbyData.amount, lobbyData.hashedSeed]
      );
    } finally {
      connection.release();
    }
  }

  static async joinLobby(lobbyId: string, opponentId: string) {
    const connection = await pool.getConnection();
    try {
      await connection.execute(
        `UPDATE rps_lobbies SET opponent_id = ?, status = 'ready' WHERE id = ? AND status = 'waiting'`,
        [opponentId, lobbyId]
      );
    } finally {
      connection.release();
    }
  }

  static async updateLobbyStatus(lobbyId: string, status: string, opponentId?: string) {
    const connection = await pool.getConnection();
    try {
      if (opponentId) {
        await connection.execute(
          `UPDATE rps_lobbies SET status = ?, opponent_id = ? WHERE id = ?`,
          [status, opponentId, lobbyId]
        );
      } else {
        await connection.execute(
          `UPDATE rps_lobbies SET status = ? WHERE id = ?`,
          [status, lobbyId]
        );
      }
    } finally {
      connection.release();
    }
  }

  static async createBattle(battleData: {
    id: string;
    lobbyId: string;
    player1Id: string;
    player2Id: string | null;
    amount: number;
    serverSeed: string;
    hashedSeed: string;
    nonce: number;
    isVsBot: boolean;
  }) {
    const connection = await pool.getConnection();
    try {
      await connection.execute(
        `INSERT INTO rps_battles (id, lobby_id, player1_id, player2_id, amount, server_seed, hashed_seed, nonce, is_vs_bot)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [battleData.id, battleData.lobbyId, battleData.player1Id, battleData.player2Id, battleData.amount, 
         battleData.serverSeed, battleData.hashedSeed, battleData.nonce, battleData.isVsBot]
      );
    } finally {
      connection.release();
    }
  }

  static async completeBattle(battleId: string, result: {
    player1Move: string;
    player2Move: string;
    winnerId: string | null;
    payout: number;
  }) {
    const connection = await pool.getConnection();
    try {
      await connection.execute(
        `UPDATE rps_battles SET 
         player1_move = ?, player2_move = ?, winner_id = ?, payout = ?
         WHERE id = ?`,
        [result.player1Move, result.player2Move, result.winnerId, result.payout, battleId]
      );
    } finally {
      connection.release();
    }
  }

  static async getBattleHistory(limit: number = 10) {
    const connection = await pool.getConnection();
    try {
      const [rows] = await connection.execute(
        `SELECT rb.*, 
         u1.username as player1_username, u1.profile_picture as player1_avatar,
         u2.username as player2_username, u2.profile_picture as player2_avatar,
         winner.username as winner_username
         FROM rps_battles rb
         LEFT JOIN users u1 ON rb.player1_id = u1.id
         LEFT JOIN users u2 ON rb.player2_id = u2.id
         LEFT JOIN users winner ON rb.winner_id = winner.id
         WHERE rb.player1_move IS NOT NULL AND rb.player2_move IS NOT NULL
         ORDER BY rb.created_at DESC
         LIMIT ?`,
        [limit]
      );
      return rows as any[];
    } finally {
      connection.release();
    }
  }

  static async getUserHistory(userId: string, limit: number = 20) {
    const connection = await pool.getConnection();
    try {
      const [rows] = await connection.execute(
        `SELECT * FROM rps_user_history 
         WHERE user_id = ? 
         ORDER BY created_at DESC 
         LIMIT ?`,
        [userId, limit]
      );
      return rows as any[];
    } finally {
      connection.release();
    }
  }

  static async getRecentBattles(limit: number = 10) {
    const connection = await pool.getConnection();
    try {
      const [rows] = await connection.execute(
        `SELECT * FROM rps_recent_battles 
         ORDER BY created_at DESC 
         LIMIT ?`,
        [limit]
      );
      return rows as any[];
    } finally {
      connection.release();
    }
  }

  static async addUserHistory(historyData: {
    id: string;
    userId: string;
    opponentId: string | null;
    opponentUsername: string | null;
    userMove: string;
    opponentMove: string;
    result: 'win' | 'lose' | 'draw';
    amount: number;
    payout: number;
    isVsBot: boolean;
  }) {
    const connection = await pool.getConnection();
    try {
      await connection.execute(
        `INSERT INTO rps_user_history (id, user_id, opponent_id, opponent_username, user_move, opponent_move, result, amount, payout, is_vs_bot)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [historyData.id, historyData.userId, historyData.opponentId, historyData.opponentUsername, 
         historyData.userMove, historyData.opponentMove, historyData.result, historyData.amount, historyData.payout, historyData.isVsBot]
      );
    } finally {
      connection.release();
    }
  }

  static async addRecentBattle(battleData: {
    id: string;
    player1Id: string;
    player1Username: string;
    player1Avatar: string | null;
    player1Move: string;
    player2Id: string | null;
    player2Username: string | null;
    player2Avatar: string | null;
    player2Move: string;
    winnerId: string | null;
    winnerUsername: string | null;
    amount: number;
    payout: number;
    isVsBot: boolean;
  }) {
    const connection = await pool.getConnection();
    try {
      await connection.execute(
        `INSERT INTO rps_recent_battles (id, player1_id, player1_username, player1_avatar, player1_move, 
         player2_id, player2_username, player2_avatar, player2_move, winner_id, winner_username, amount, payout, is_vs_bot)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [battleData.id, battleData.player1Id, battleData.player1Username, battleData.player1Avatar, battleData.player1Move,
         battleData.player2Id, battleData.player2Username, battleData.player2Avatar, battleData.player2Move,
         battleData.winnerId, battleData.winnerUsername, battleData.amount, battleData.payout, battleData.isVsBot]
      );

      await connection.execute(
        `DELETE FROM rps_recent_battles WHERE id NOT IN (
          SELECT id FROM (
            SELECT id FROM rps_recent_battles ORDER BY created_at DESC LIMIT 50
          ) AS temp
        )`
      );
    } finally {
      connection.release();
    }
  }
}

// Chat operations
export class ChatDatabase {
  static async saveMessage(messageData: {
    id: string;
    userId: string;
    message: string;
    isSystemMessage?: boolean;
  }) {
    const connection = await pool.getConnection();
    try {
      await connection.execute(
        `INSERT INTO chat_messages (id, user_id, message, is_system_message)
         VALUES (?, ?, ?, ?)`,
        [messageData.id, messageData.userId, messageData.message, messageData.isSystemMessage || false]
      );
    } finally {
      connection.release();
    }
  }

  static async getRecentMessages(limit: number = 100) {
    const connection = await pool.getConnection();
    try {
      const [rows] = await connection.execute(
        `SELECT cm.*, u.username, u.profile_picture
         FROM chat_messages cm
         LEFT JOIN users u ON cm.user_id = u.id
         ORDER BY cm.created_at DESC
         LIMIT ?`,
        [limit]
      );
      return (rows as any[]).reverse();
    } finally {
      connection.release();
    }
  }
}

// Game Database for backwards compatibility
export class GameDatabase {
  static async createDiceGame(gameData: {
    id: string;
    serverSeed: string;
    hashedSeed: string;
    publicSeed: string;
    nonce: number;
  }) {
    return DiceDatabase.createGame(gameData);
  }

  static async updateDiceGameResult(gameId: string, diceValue: number, isOdd: boolean) {
    // This method might need additional parameters for total wagered, payout, and players count
    // You may need to calculate these values before calling completeGame
    return DiceDatabase.completeGame(gameId, {
      diceValue,
      isOdd,
      totalWagered: 0, // You'll need to calculate this
      totalPayout: 0,  // You'll need to calculate this
      playersCount: 0  // You'll need to calculate this
    });
  }

  static async createDiceBet(betData: {
    id: string;
    gameId: string;
    userId: string;
    amount: number;
    choice: 'odd' | 'even';
  }) {
    return DiceDatabase.placeBet(betData);
  }

  static async updateDiceBetResult(betId: string, isWinner: boolean, payout: number) {
    return DiceDatabase.updateBetResult(betId, isWinner, payout);
  }

  static async getDiceGameHistory(limit: number = 10) {
    return DiceDatabase.getGameHistory(limit);
  }
}

// Admin Database for dashboard statistics  
export class AdminDatabase {
  static async getDashboardStats() {
    const connection = await pool.getConnection();
    try {
      // Get user stats
      const [userStats] = await connection.execute(`
        SELECT 
          COUNT(*) as total_users,
          SUM(CASE WHEN last_active > DATE_SUB(NOW(), INTERVAL 24 HOUR) THEN 1 ELSE 0 END) as active_users,
          SUM(CASE WHEN DATE(created_at) = CURDATE() THEN 1 ELSE 0 END) as new_today
        FROM users
        WHERE has_beta_access = 1
      `);

      // Get financial stats
      const [financialStats] = await connection.execute(`
        SELECT 
          SUM(total_wagered - total_won) as total_revenue,
          SUM(CASE WHEN DATE(last_active) = CURDATE() THEN total_wagered - total_won ELSE 0 END) as today_revenue
        FROM users
        WHERE has_beta_access = 1
      `);

      // Get dice game stats
      const [diceStats] = await connection.execute(`
        SELECT 
          COUNT(*) as total_games,
          SUM(total_wagered) as total_wagered,
          SUM(total_payout) as total_payout
        FROM dice_games
        WHERE status = 'complete'
      `);

      // Get crash game stats
      const [crashStats] = await connection.execute(`
        SELECT 
          COUNT(*) as total_games,
          SUM(total_wagered) as total_wagered,
          SUM(total_payout) as total_payout
        FROM crash_games
        WHERE status = 'complete'
      `);

      return {
        users: (userStats as any[])[0],
        financial: (financialStats as any[])[0],
        dice: (diceStats as any[])[0],
        crash: (crashStats as any[])[0]
      };
    } finally {
      connection.release();
    }
  }
}

export default pool;
