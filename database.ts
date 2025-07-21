// /Users/macbook/Documents/n1verse/src/lib/database.ts
import mysql from 'mysql2/promise';

const pool = mysql.createPool({
  host: process.env.DB_HOST?.split(':')[0] || 'db-fde-02.sparkedhost.us',
  port: parseInt(process.env.DB_HOST?.split(':')[1] || '3306'),
  user: process.env.DB_USER || 'u175260_2aWtznM6FW',
  password: process.env.DB_PASSWORD || 'giqaKuZnR72ZdQL=m.DVdtUB',
  database: process.env.DB_NAME || 's175260_casino-n1verse',
  waitForConnections: true,
});

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
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        is_active BOOLEAN DEFAULT TRUE,
        INDEX idx_wallet (wallet_address),
        INDEX idx_username (username),
        INDEX idx_created_at (created_at)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);

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
        game_type ENUM('dice', 'rps') NOT NULL,
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

// User operations
export class UserDatabase {
  static async createUser(userData: {
    id: string;
    username: string;
    email?: string;
    walletAddress: string;
    referralCode?: string;
    profilePicture: string;
  }) {
    const connection = await pool.getConnection();
    try {
      await connection.execute(
        `INSERT INTO users (id, username, email, wallet_address, referral_code, profile_picture)
         VALUES (?, ?, ?, ?, ?, ?)`,
        [userData.id, userData.username, userData.email || null, userData.walletAddress, userData.referralCode || null, userData.profilePicture]
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

  static async updateUserBalance(userId: string, amount: number, operation: 'add' | 'subtract' = 'add') {
    const connection = await pool.getConnection();
    try {
      const operator = operation === 'add' ? '+' : '-';
      await connection.execute(
        `UPDATE users SET balance = balance ${operator} ?, last_active = CURRENT_TIMESTAMP WHERE id = ?`,
        [Math.abs(amount), userId]
      );
      
      return await this.getUserById(userId);
    } finally {
      connection.release();
    }
  }

  static async updateUserStats(userId: string, wagered: number, won: number) {
    const connection = await pool.getConnection();
    try {
      await connection.execute(
        `UPDATE users SET 
         total_wagered = total_wagered + ?,
         total_won = total_won + ?,
         games_played = games_played + 1,
         last_active = CURRENT_TIMESTAMP
         WHERE id = ?`,
        [wagered, won, userId]
      );
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

  // Get user-specific battle history from rps_user_history table
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

  // Get recent public battles from rps_recent_battles table
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

  // Add user-specific battle history entry
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

  // Add recent public battle entry
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

      // Keep only the latest 50 recent battles
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
      return (rows as any[]).reverse(); // Return in chronological order
    } finally {
      connection.release();
    }
  }
}

export default pool;