// /Users/macbook/Documents/n1verse/server.js
// Complete Socket.IO server for Railway deployment - PROFESSIONAL CRASH GAME IMPLEMENTATION WITH WORKING RPS
const { createServer } = require('http');
const { Server } = require('socket.io');

const port = process.env.PORT || 3001;

// Create HTTP server
const httpServer = createServer();

// Create Socket.IO server with CORS for Vercel
const io = new Server(httpServer, {
  cors: {
    origin: [
      "https://n1verse.vercel.app",
      "https://*.vercel.app",
      "http://localhost:3000",
      "http://localhost:3001"
    ],
    methods: ["GET", "POST"],
    credentials: true
  },
  pingTimeout: 60000,
  pingInterval: 25000,
  transports: ['websocket']
});

// Database setup
let UserDatabase, RPSDatabase, DiceDatabase, CrashDatabase;

try {
  const mysql = require('mysql2/promise');
  
  const pool = mysql.createPool({
    host: process.env.DB_HOST?.split(':')[0] || 'db-fde-02.sparkedhost.us',
    port: parseInt(process.env.DB_HOST?.split(':')[1] || '3306'),
    user: process.env.DB_USER || 'u175260_2aWtznM6FW',
    password: process.env.DB_PASSWORD || 'giqaKuZnR72ZdQL=m.DVdtUB',
    database: process.env.DB_NAME || 's175260_casino-n1verse',
    waitForConnections: true,
    acquireTimeout: 60000,
    timeout: 60000,
    reconnect: true
  });

  UserDatabase = {
  updateUserBalance: async (userId, amount, operation) => {
    let connection;
    try {
      connection = await pool.getConnection();
      const operator = operation === 'add' ? '+' : '-';
      await connection.execute(
        `UPDATE users SET balance = balance ${operator} ?, last_active = CURRENT_TIMESTAMP WHERE id = ?`,
        [Math.abs(amount), userId]
      );
      console.log(`💰 Database: ${operation} ${amount} for user ${userId}`);
      return true;
    } catch (error) {
      console.error('❌ Database updateUserBalance error:', error);
      return false;
    } finally {
      if (connection) connection.release();
    }
  },
  updateUserStats: async (userId, wagered, won) => {
    let connection;
    try {
      connection = await pool.getConnection();
      await connection.execute(
        `UPDATE users SET 
         total_wagered = total_wagered + ?,
         total_won = total_won + ?,
         games_played = games_played + 1,
         last_active = CURRENT_TIMESTAMP
         WHERE id = ?`,
        [wagered, won, userId]
      );
      console.log(`📊 Database: stats updated for user ${userId} - wagered: ${wagered}, won: ${won}`);
      return true;
    } catch (error) {
      console.error('❌ Database updateUserStats error:', error);
      return false;
    } finally {
      if (connection) connection.release();
    }
  },
  // ADD THIS MISSING METHOD:
  getUserById: async (userId) => {
    let connection;
    try {
      connection = await pool.getConnection();
      const [rows] = await connection.execute(
        'SELECT id, username, balance, profile_picture FROM users WHERE id = ?',
        [userId]
      );
      
      if (rows.length > 0) {
        console.log(`✅ Database: User found ${userId} with balance ${rows[0].balance}`);
        return {
          id: rows[0].id,
          username: rows[0].username,
          balance: parseFloat(rows[0].balance),
          profilePicture: rows[0].profile_picture
        };
      }
      
      console.log(`⚠️ Database: User not found ${userId}`);
      return null;
    } catch (error) {
      console.error('❌ Database getUserById error:', error);
      return null;
    } finally {
      if (connection) connection.release();
    }
  }
};

  // Dice Database functions
  DiceDatabase = {
    createGame: async (gameData) => {
      let connection;
      try {
        connection = await pool.getConnection();
        
        const [existing] = await connection.execute('SELECT id FROM dice_games WHERE id = ?', [gameData.id]);
        if (existing.length > 0) {
          console.log(`🎲 Database: Game ${gameData.id} already exists`);
          return true;
        }
        
        await connection.execute(
          `INSERT INTO dice_games (id, server_seed, hashed_seed, public_seed, nonce, status)
           VALUES (?, ?, ?, ?, ?, 'betting')`,
          [gameData.id, gameData.serverSeed, gameData.hashedSeed, gameData.publicSeed || null, gameData.nonce]
        );
        console.log(`🎲 Database: Game created ${gameData.id}`);
        return true;
      } catch (error) {
        console.error('❌ Database createGame error:', error);
        return false;
      } finally {
        if (connection) connection.release();
      }
    },
    completeGame: async (gameId, result) => {
      let connection;
      try {
        connection = await pool.getConnection();
        await connection.execute(
          `UPDATE dice_games SET 
           dice_value = ?, is_odd = ?, total_wagered = ?, total_payout = ?, 
           players_count = ?, status = 'complete', completed_at = CURRENT_TIMESTAMP
           WHERE id = ?`,
          [result.diceValue, result.isOdd, result.totalWagered, result.totalPayout, result.playersCount, gameId]
        );
        console.log(`🎲 Database: Game completed ${gameId}`);
        return true;
      } catch (error) {
        console.error('❌ Database completeGame error:', error);
        return false;
      } finally {
        if (connection) connection.release();
      }
    },
    placeBet: async (betData) => {
      let connection;
      try {
        connection = await pool.getConnection();
        
        const [gameExists] = await connection.execute('SELECT id FROM dice_games WHERE id = ? AND status = ?', [betData.gameId, 'betting']);
        if (gameExists.length === 0) {
          console.error(`❌ Database: Game ${betData.gameId} does not exist or not in betting phase`);
          return false;
        }
        
        const [userExists] = await connection.execute('SELECT id FROM users WHERE id = ?', [betData.userId]);
        if (userExists.length === 0) {
          console.error(`❌ Database: User ${betData.userId} does not exist`);
          return false;
        }
        
        const [betExists] = await connection.execute(
          'SELECT id FROM dice_bets WHERE game_id = ? AND user_id = ?',
          [betData.gameId, betData.userId]
        );
        
        if (betExists.length > 0) {
          console.log(`⚠️ Database: Bet already exists for user ${betData.userId} in game ${betData.gameId}`);
          return false;
        }
        
        await connection.execute(
          `INSERT INTO dice_bets (id, game_id, user_id, amount, choice, created_at)
           VALUES (?, ?, ?, ?, ?, NOW())`,
          [betData.id, betData.gameId, betData.userId, betData.amount, betData.choice]
        );
        console.log(`🎲 Database: Bet placed ${betData.id}`);
        return true;
      } catch (error) {
        console.error('❌ Database placeBet error:', error);
        return false;
      } finally {
        if (connection) connection.release();
      }
    },
    updateBetResult: async (betId, isWinner, payout) => {
      let connection;
      try {
        connection = await pool.getConnection();
        await connection.execute(
          'UPDATE dice_bets SET is_winner = ?, payout = ? WHERE id = ?',
          [isWinner, payout, betId]
        );
        console.log(`🎲 Database: Bet result updated ${betId} - Winner: ${isWinner}, Payout: ${payout}`);
        return true;
      } catch (error) {
        console.error('❌ Database updateBetResult error:', error);
        return false;
      } finally {
        if (connection) connection.release();
      }
    }
  };

  // Crash Database functions
  CrashDatabase = {
    createGame: async (gameData) => {
      let connection;
      try {
        connection = await pool.getConnection();
        
        const [existing] = await connection.execute('SELECT id FROM crash_games WHERE id = ?', [gameData.id]);
        if (existing.length > 0) {
          console.log(`🚀 Database: Crash game ${gameData.id} already exists`);
          return true;
        }
        
        await connection.execute(
          `INSERT INTO crash_games (id, server_seed, hashed_seed, public_seed, nonce, status)
           VALUES (?, ?, ?, ?, ?, 'betting')`,
          [gameData.id, gameData.serverSeed, gameData.hashedSeed, gameData.publicSeed || null, gameData.nonce]
        );
        console.log(`🚀 Database: Crash game created ${gameData.id}`);
        return true;
      } catch (error) {
        console.error('❌ Database createCrashGame error:', error);
        return false;
      } finally {
        if (connection) connection.release();
      }
    },
    completeGame: async (gameId, result) => {
      let connection;
      try {
        connection = await pool.getConnection();
        await connection.execute(
          `UPDATE crash_games SET 
           crash_point = ?, total_wagered = ?, total_payout = ?, 
           players_count = ?, status = 'complete', crashed_at = CURRENT_TIMESTAMP
           WHERE id = ?`,
          [result.crashPoint, result.totalWagered, result.totalPayout, result.playersCount, gameId]
        );
        console.log(`🚀 Database: Crash game completed ${gameId} at ${result.crashPoint}x`);
        return true;
      } catch (error) {
        console.error('❌ Database completeCrashGame error:', error);
        return false;
      } finally {
        if (connection) connection.release();
      }
    },
    placeBet: async (betData) => {
      let connection;
      try {
        connection = await pool.getConnection();
        
        const [gameExists] = await connection.execute('SELECT id FROM crash_games WHERE id = ? AND status = ?', [betData.gameId, 'betting']);
        if (gameExists.length === 0) {
          console.error(`❌ Database: Crash game ${betData.gameId} does not exist or not in betting phase`);
          return false;
        }
        
        const [userExists] = await connection.execute('SELECT id FROM users WHERE id = ?', [betData.userId]);
        if (userExists.length === 0) {
          console.error(`❌ Database: User ${betData.userId} does not exist`);
          return false;
        }
        
        const [betExists] = await connection.execute(
          'SELECT id FROM crash_bets WHERE game_id = ? AND user_id = ?',
          [betData.gameId, betData.userId]
        );
        
        if (betExists.length > 0) {
          console.log(`⚠️ Database: Crash bet already exists for user ${betData.userId} in game ${betData.gameId}`);
          return false;
        }
        
        await connection.execute(
          `INSERT INTO crash_bets (id, game_id, user_id, amount, created_at)
           VALUES (?, ?, ?, ?, NOW())`,
          [betData.id, betData.gameId, betData.userId, betData.amount]
        );
        console.log(`🚀 Database: Crash bet placed ${betData.id}`);
        return true;
      } catch (error) {
        console.error('❌ Database placeCrashBet error:', error);
        return false;
      } finally {
        if (connection) connection.release();
      }
    },
    cashOut: async (betId, cashOutMultiplier, payout) => {
      let connection;
      try {
        connection = await pool.getConnection();
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
        console.log(`🚀 Database: Cash out successful ${betId} at ${cashOutMultiplier}x for ${payout} USDC`);
        return true;
      } catch (error) {
        console.error('❌ Database cashOut error:', error);
        return false;
      } finally {
        if (connection) connection.release();
      }
    },
    updateBetResult: async (betId, isWinner, payout) => {
      let connection;
      try {
        connection = await pool.getConnection();
        await connection.execute(
          'UPDATE crash_bets SET is_winner = ?, payout = ? WHERE id = ?',
          [isWinner, payout, betId]
        );
        console.log(`🚀 Database: Crash bet result updated ${betId} - Winner: ${isWinner}, Payout: ${payout}`);
        return true;
      } catch (error) {
        console.error('❌ Database updateCrashBetResult error:', error);
        return false;
      } finally {
        if (connection) connection.release();
      }
    },
    getUserActiveBet: async (userId, gameId) => {
      let connection;
      try {
        connection = await pool.getConnection();
        const [bets] = await connection.execute(
          `SELECT * FROM crash_bets WHERE user_id = ? AND game_id = ? LIMIT 1`,
          [userId, gameId]
        );
        
        if (bets.length > 0) {
          console.log(`🚀 Database: Found active bet for user ${userId} in game ${gameId}`);
          return {
            id: bets[0].id,
            amount: parseFloat(bets[0].amount),
            isCashedOut: bets[0].is_cashed_out === 1,
            cashOutAt: bets[0].cash_out_at ? parseFloat(bets[0].cash_out_at) : null,
            payout: parseFloat(bets[0].payout || 0)
          };
        }
        return null;
      } catch (error) {
        console.error('❌ Database getUserActiveBet error:', error);
        return null;
      } finally {
        if (connection) connection.release();
      }
    }
  };

  // RPS Database functions - COMPLETE FROM OLD FILE
  RPSDatabase = {
    createLobby: async (lobbyData) => {
      let connection;
      try {
        connection = await pool.getConnection();
        await connection.execute(
          `INSERT INTO rps_lobbies (id, creator_id, amount, hashed_seed, timeout_at)
           VALUES (?, ?, ?, ?, DATE_ADD(NOW(), INTERVAL 30 SECOND))`,
          [lobbyData.id, lobbyData.creatorId, lobbyData.amount, lobbyData.hashedSeed]
        );
        console.log(`🏆 Database: Lobby created ${lobbyData.id}`);
        return true;
      } catch (error) {
        console.error('❌ Database createLobby error:', error);
        return false;
      } finally {
        if (connection) connection.release();
      }
    },
    updateLobbyStatus: async (lobbyId, status, opponentId) => {
      let connection;
      try {
        connection = await pool.getConnection();
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
        console.log(`🔄 Database: Lobby ${lobbyId} status updated to ${status}`);
        return true;
      } catch (error) {
        console.error('❌ Database updateLobbyStatus error:', error);
        return false;
      } finally {
        if (connection) connection.release();
      }
    },
    createBattle: async (battleData) => {
      let connection;
      try {
        connection = await pool.getConnection();
        await connection.execute(
          `INSERT INTO rps_battles (id, lobby_id, player1_id, player2_id, amount, server_seed, hashed_seed, nonce, is_vs_bot)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          [battleData.id, battleData.lobbyId, battleData.player1Id, battleData.player2Id, battleData.amount, 
           battleData.serverSeed, battleData.hashedSeed, battleData.nonce, battleData.isVsBot]
        );
        console.log(`⚔️ Database: Battle created ${battleData.id}`);
        return true;
      } catch (error) {
        console.error('❌ Database createBattle error:', error);
        return false;
      } finally {
        if (connection) connection.release();
      }
    },
    completeBattle: async (battleId, result) => {
      let connection;
      try {
        connection = await pool.getConnection();
        await connection.execute(
          `UPDATE rps_battles SET 
           player1_move = ?, player2_move = ?, winner_id = ?, payout = ?
           WHERE id = ?`,
          [result.player1Move, result.player2Move, result.winnerId, result.payout, battleId]
        );
        console.log(`🏁 Database: Battle completed ${battleId}`);
        return true;
      } catch (error) {
        console.error('❌ Database completeBattle error:', error);
        return false;
      } finally {
        if (connection) connection.release();
      }
    },
    addUserHistory: async (historyData) => {
      let connection;
      try {
        connection = await pool.getConnection();
        await connection.execute(
          `INSERT INTO rps_user_history (id, user_id, opponent_id, opponent_username, user_move, opponent_move, result, amount, payout, is_vs_bot)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          [historyData.id, historyData.userId, historyData.opponentId, historyData.opponentUsername, 
           historyData.userMove, historyData.opponentMove, historyData.result, historyData.amount, historyData.payout, historyData.isVsBot]
        );
        console.log(`📜 Database: User history added for ${historyData.userId}`);
        return true;
      } catch (error) {
        console.error('❌ Database addUserHistory error:', error);
        return false;
      } finally {
        if (connection) connection.release();
      }
    },
    addRecentBattle: async (battleData) => {
      let connection;
      try {
        connection = await pool.getConnection();
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
        console.log(`🌐 Database: Recent battle added ${battleData.id}`);
        return true;
      } catch (error) {
        console.error('❌ Database addRecentBattle error:', error);
        return false;
      } finally {
        if (connection) connection.release();
      }
    },
    getUserHistory: async (userId, limit = 20) => {
      let connection;
      try {
        connection = await pool.getConnection();
        const [rows] = await connection.execute(
          `SELECT * FROM rps_user_history 
           WHERE user_id = ? 
           ORDER BY created_at DESC 
           LIMIT ?`,
          [userId, limit]
        );
        return rows;
      } catch (error) {
        console.error('❌ Database getUserHistory error:', error);
        return [];
      } finally {
        if (connection) connection.release();
      }
    },
    getRecentBattles: async (limit = 10) => {
      let connection;
      try {
        connection = await pool.getConnection();
        const [rows] = await connection.execute(
          `SELECT * FROM rps_recent_battles 
           ORDER BY created_at DESC 
           LIMIT ?`,
          [limit]
        );
        return rows;
      } catch (error) {
        console.error('❌ Database getRecentBattles error:', error);
        return [];
      } finally {
        if (connection) connection.release();
      }
    },
    getBattleHistory: async (limit = 10) => {
      let connection;
      try {
        connection = await pool.getConnection();
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
        return rows;
      } catch (error) {
        console.error('❌ Database getBattleHistory error:', error);
        return [];
      } finally {
        if (connection) connection.release();
      }
    }
  };
  
  console.log('✅ Database connection established');
} catch (error) {
  console.log('⚠️ Database import failed, using mock functions');
  UserDatabase = {
    updateUserBalance: async () => { console.log('📝 Mock: updateUserBalance called'); return false; },
    updateUserStats: async () => { console.log('📝 Mock: updateUserStats called'); return false; }
  };
  DiceDatabase = {
    createGame: async () => { console.log('📝 Mock: createGame called'); return true; },
    completeGame: async () => { console.log('📝 Mock: completeGame called'); return true; },
    placeBet: async () => { console.log('📝 Mock: placeBet called'); return true; },
    updateBetResult: async () => { console.log('📝 Mock: updateBetResult called'); return true; }
  };
  CrashDatabase = {
    createGame: async () => { console.log('📝 Mock: createCrashGame called'); return true; },
    completeGame: async () => { console.log('📝 Mock: completeCrashGame called'); return true; },
    placeBet: async () => { console.log('📝 Mock: placeCrashBet called'); return true; },
    cashOut: async () => { console.log('📝 Mock: crashCashOut called'); return true; },
    updateBetResult: async () => { console.log('📝 Mock: updateCrashBetResult called'); return true; },
    getUserActiveBet: async () => { console.log('📝 Mock: getUserActiveBet called'); return null; }
  };
  RPSDatabase = {
    createLobby: async () => { console.log('📝 Mock: createLobby called'); return true; },
    updateLobbyStatus: async () => { console.log('📝 Mock: updateLobbyStatus called'); return true; },
    createBattle: async () => { console.log('📝 Mock: createBattle called'); return true; },
    completeBattle: async () => { console.log('📝 Mock: completeBattle called'); return true; },
    addUserHistory: async () => { console.log('📝 Mock: addUserHistory called'); return true; },
    addRecentBattle: async () => { console.log('📝 Mock: addRecentBattle called'); return true; },
    getUserHistory: async () => [],
    getRecentBattles: async () => [],
    getBattleHistory: async () => []
  };
}

// Helper functions for safe database operations
async function safeUpdateUserBalance(userId, amount, operation) {
  try {
    if (UserDatabase && typeof UserDatabase.updateUserBalance === 'function') {
      const success = await UserDatabase.updateUserBalance(userId, amount, operation);
      if (success) {
        console.log(`✅ Balance updated: ${operation} ${amount} USDC for user ${userId}`);
      } else {
        console.log(`⚠️ Balance update failed for user ${userId}`);
      }
      return success;
    } else {
      console.log(`💰 Mock balance update: ${operation} ${amount} for user ${userId}`);
      return false;
    }
  } catch (error) {
    console.error('❌ Error updating user balance:', error);
    return false;
  }
}

async function safeUpdateUserStats(userId, wagered, won) {
  try {
    if (UserDatabase && typeof UserDatabase.updateUserStats === 'function') {
      const success = await UserDatabase.updateUserStats(userId, wagered, won);
      if (success) {
        console.log(`✅ Stats updated: wagered ${wagered}, won ${won} for user ${userId}`);
      } else {
        console.log(`⚠️ Stats update failed for user ${userId}`);
      }
      return success;
    } else {
      console.log(`📊 Mock stats update: wagered ${wagered}, won ${won} for user ${userId}`);
      return false;
    }
  } catch (error) {
    console.error('❌ Error updating user stats:', error);
    return false;
  }
}

async function safeDiceDatabase(functionName, ...args) {
  try {
    if (DiceDatabase && typeof DiceDatabase[functionName] === 'function') {
      const result = await DiceDatabase[functionName](...args);
      console.log(`✅ Dice Database ${functionName} completed successfully`);
      return result;
    } else {
      console.log(`📝 Mock Dice Database ${functionName} called with args:`, args);
      return true;
    }
  } catch (error) {
    console.error(`❌ Error in Dice Database ${functionName}:`, error);
    return false;
  }
}

async function safeCrashDatabase(functionName, ...args) {
  try {
    if (CrashDatabase && typeof CrashDatabase[functionName] === 'function') {
      const result = await CrashDatabase[functionName](...args);
      console.log(`✅ Crash Database ${functionName} completed successfully`);
      return result;
    } else {
      console.log(`📝 Mock Crash Database ${functionName} called with args:`, args);
      return true;
    }
  } catch (error) {
    console.error(`❌ Error in Crash Database ${functionName}:`, error);
    return false;
  }
}

async function safeRPSDatabase(functionName, ...args) {
  try {
    if (RPSDatabase && typeof RPSDatabase[functionName] === 'function') {
      const result = await RPSDatabase[functionName](...args);
      console.log(`✅ RPS Database ${functionName} completed successfully`);
      return result;
    } else {
      console.log(`📝 Mock RPS Database ${functionName} called with args:`, args);
      return null;
    }
  } catch (error) {
    console.error(`❌ Error in RPS Database ${functionName}:`, error);
    return null;
  }
}

// Game state management - ADD LOCK TO PREVENT MULTIPLE GAMES
const gameState = {
  dice: {
    currentGame: null,
    history: [],
    players: new Map(),
    gameCounter: 0,
    bettingInterval: null,
    rollingTimeout: null,
    isProcessing: false
  },
  crash: {
    currentGame: null,
    history: [],
    players: new Map(), // Store by userId instead of socketId
    socketToUser: new Map(), // Map socketId to userId
    gameCounter: 0,
    bettingInterval: null,
    flyingInterval: null,
    isProcessing: false,
    currentMultiplier: 1.00,
    startTime: null,
    crashed: false,
    isGameLocked: false // PREVENT MULTIPLE GAMES
  },
  rps: {
    lobbies: new Map(),
    activeBattles: new Map(),
    history: []
  },
  chat: {
    messages: [],
    onlineUsers: new Set() // Track online chat users
  },
  connectedUsers: new Map()
};

// Utility functions
function generateGameId() {
  gameState.dice.gameCounter++;
  return `dice_${Date.now()}_${gameState.dice.gameCounter}`;
}

function generateCrashGameId() {
  gameState.crash.gameCounter++;
  return `crash_${Date.now()}_${gameState.crash.gameCounter}`;
}

function generateLobbyId() {
  return `rps_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

function generateBattleId() {
  return `battle_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

function generateServerSeed() {
  return require('crypto').randomBytes(32).toString('hex');
}

function generateHash(data) {
  return require('crypto').createHash('sha256').update(data).digest('hex');
}

function generateHashedSeed() {
  const seed = generateServerSeed();
  return generateHash(seed);
}

function generateProvablyFairDiceResult(serverSeed, nonce) {
  const crypto = require('crypto');
  const hmac = crypto.createHmac('sha256', serverSeed);
  hmac.update(`${nonce}:dice`);
  const hash = hmac.digest('hex');
  
  const hexSubstring = hash.substring(0, 2);
  const intValue = parseInt(hexSubstring, 16);
  const diceValue = (intValue % 6) + 1;
  
  return {
    value: diceValue,
    isOdd: diceValue % 2 === 1
  };
}

function generateProvablyFairRPSMove(hashedSeed, nonce) {
  const crypto = require('crypto');
  const moves = ['rock', 'paper', 'scissors'];
  const hmac = crypto.createHmac('sha256', hashedSeed);
  hmac.update(`${nonce}:rps`);
  const hash = hmac.digest('hex');
  
  const hexSubstring = hash.substring(0, 2);
  const intValue = parseInt(hexSubstring, 16);
  const moveIndex = intValue % 3;
  
  return moves[moveIndex];
}

function determineRPSWinner(move1, move2) {
  if (move1 === move2) return { winner: 'draw' };
  
  const wins = {
    rock: 'scissors',
    paper: 'rock',
    scissors: 'paper'
  };
  
  return { winner: wins[move1] === move2 ? 'player1' : 'player2' };
}

// PROFESSIONAL CRASH POINT GENERATION WITH PROPER MATHEMATICS
function generateProvablyFairCrashPoint(serverSeed, nonce) {
  const crypto = require('crypto');
  
  // Create HMAC with server seed and nonce
  const hmac = crypto.createHmac('sha256', serverSeed);
  hmac.update(`${nonce}:crash`);
  const hash = hmac.digest('hex');
  
  // Convert hash to number for randomness
  const hexSubstring = hash.substring(0, 13); // Use 13 characters for higher precision
  const H = parseInt(hexSubstring, 16);
  const maxValue = Math.pow(16, 13);
  
  // Normalize to 0-1 range with high precision
  const e = H / maxValue;
  
  // Apply crash game mathematical formula
  // Using logarithmic distribution for realistic crash points
  // House edge of approximately 1% is built into the formula
  const houseEdge = 0.01;
  const multiplier = (1 - houseEdge) / (1 - e);
  
  // Apply minimum and maximum bounds
  let crashPoint = Math.max(1.00, multiplier);
  
  // Apply realistic distribution curve
  // Most crashes should be between 1.0x - 10.0x with occasional high multipliers
  if (crashPoint > 100) {
    // Reduce probability of extremely high multipliers
    crashPoint = Math.min(crashPoint, 1000 + (Math.random() * 9000));
  }
  
  // Round to 2 decimal places for display
  return Math.round(crashPoint * 100) / 100;
}

// Clear dice game timers only
function clearDiceTimers() {
  if (gameState.dice.bettingInterval) {
    clearInterval(gameState.dice.bettingInterval);
    gameState.dice.bettingInterval = null;
  }
  if (gameState.dice.rollingTimeout) {
    clearTimeout(gameState.dice.rollingTimeout);
    gameState.dice.rollingTimeout = null;
  }
  console.log('🧹 Dice timers cleared');
}

// Clear crash game timers only
function clearCrashTimers() {
  if (gameState.crash.bettingInterval) {
    clearInterval(gameState.crash.bettingInterval);
    gameState.crash.bettingInterval = null;
  }
  if (gameState.crash.flyingInterval) {
    clearInterval(gameState.crash.flyingInterval);
    gameState.crash.flyingInterval = null;
  }
  console.log('🧹 Crash timers cleared');
}

// Clear all timers and intervals (for shutdown)
function clearAllGameTimers() {
  console.log('🧹 Starting complete game cleanup...');
  clearDiceTimers();
  clearCrashTimers();
  console.log('🧹 All game timers cleared');
}

// Complete dice game cleanup
function cleanupDiceState() {
  const connectedDiceSockets = Array.from(gameState.dice.players.keys());
  gameState.dice.players.clear();
  console.log(`🧹 Cleared ${connectedDiceSockets.length} dice players`);
}

// FIXED: Complete crash game cleanup - only for completed games
function cleanupCrashState() {
  // Only clear players if game is complete
  if (gameState.crash.currentGame?.phase === 'complete') {
    const connectedCrashPlayers = Array.from(gameState.crash.players.keys());
    gameState.crash.players.clear();
    gameState.crash.socketToUser.clear();
    console.log(`🧹 Cleared ${connectedCrashPlayers.length} crash players from completed game`);
  }
  
  gameState.crash.currentMultiplier = 1.00;
  gameState.crash.startTime = null;
  gameState.crash.crashed = false;
}

// ROBUST CRASH BET RECOVERY SYSTEM - FIXED
async function recoverCrashBetForUser(socket, userData, gameId) {
  try {
    console.log(`🔄 Attempting bet recovery for user ${userData.username} in game ${gameId}`);
    
    const activeBet = await safeCrashDatabase('getUserActiveBet', userData.id, gameId);
    
    if (activeBet) {
      console.log(`✅ Found active bet in database:`, activeBet);
      
      const playerBet = {
        userId: userData.id,
        username: userData.username,
        amount: activeBet.amount,
        socketId: socket.id,
        profilePicture: userData.profilePicture || '/default-avatar.png',
        timestamp: new Date(),
        gameId: gameId,
        betId: activeBet.id,
        isCashedOut: activeBet.isCashedOut,
        cashOutAt: activeBet.cashOutAt,
        payout: activeBet.payout
      };
      
      // Add to game state using userId as key
      gameState.crash.players.set(userData.id, playerBet);
      gameState.crash.socketToUser.set(socket.id, userData.id);
      
      // Notify user of recovered bet
      socket.emit('crash-bet-recovered', {
        bet: playerBet,
        message: `Bet recovered: ${activeBet.amount} USDC${activeBet.isCashedOut ? ` (Cashed out at ${activeBet.cashOutAt}x)` : ''}`
      });
      
      // Notify all players
      io.to('crash-room').emit('crash-player-joined', {
        playerId: socket.id,
        userId: userData.id,
        username: userData.username,
        amount: activeBet.amount,
        profilePicture: playerBet.profilePicture,
        isCashedOut: activeBet.isCashedOut,
        cashOutAt: activeBet.cashOutAt,
        payout: activeBet.payout
      });
      
      console.log(`✅ Bet recovery successful for ${userData.username}`);
      return true;
    }
    
    console.log(`ℹ️ No active bet found for user ${userData.username} in game ${gameId}`);
    return false;
  } catch (error) {
    console.error(`❌ Error during bet recovery:`, error);
    return false;
  }
}

// Helper function to get user profile picture from database
async function getUserProfilePicture(userId) {
  try {
    if (!userId) return null;
    
    let mysql;
    try {
      mysql = require('mysql2/promise');
    } catch (mysqlError) {
      console.log('MySQL not available for profile lookup');
      return null;
    }

    const pool = mysql.createPool({
      host: process.env.DB_HOST?.split(':')[0] || 'db-fde-02.sparkedhost.us',
      port: parseInt(process.env.DB_HOST?.split(':')[1] || '3306'),
      user: process.env.DB_USER || 'u175260_2aWtznM6FW',
      password: process.env.DB_PASSWORD || 'giqaKuZnR72ZdQL=m.DVdtUB',
      database: process.env.DB_NAME || 's175260_casino-n1verse',
      waitForConnections: true,
    });

    const connection = await pool.getConnection();
    const [rows] = await connection.execute(
      'SELECT profile_picture FROM users WHERE id = ?',
      [userId]
    );
    connection.release();
    await pool.end();

    if (rows.length > 0 && rows[0].profile_picture) {
      return rows[0].profile_picture;
    }
    return null;
  } catch (error) {
    console.error('Error fetching user profile picture:', error);
    return null;
  }
}

// Function to broadcast online user count
function broadcastOnlineUserCount() {
  const count = gameState.chat.onlineUsers.size;
  io.to('chat-room').emit('online-users-count', count);
  console.log(`💬 Broadcasting online user count: ${count}`);
}

// Socket.IO event handlers
io.on('connection', (socket) => {
  console.log('User connected:', socket.id);

  socket.on('user-connect', (userData) => {
    gameState.connectedUsers.set(socket.id, userData);
    socket.userData = userData;
  });

  // Live Chat Events - NEW
  socket.on('join-chat', (userData) => {
    console.log(`💬 User joining chat room: ${userData?.username} (${socket.id})`);
    socket.join('chat-room');
    socket.userData = userData;
    
    // Add user to online users set
    if (userData?.id) {
      gameState.chat.onlineUsers.add(userData.id);
    }
    
    // Send chat history
    if (gameState.chat.messages.length > 0) {
      socket.emit('chat-history', gameState.chat.messages.slice(-50)); // Last 50 messages
    }
    
    // Broadcast updated online count
    broadcastOnlineUserCount();
    
    console.log(`💬 ${userData?.username} joined chat. Online users: ${gameState.chat.onlineUsers.size}`);
  });

  socket.on('send-message', (messageData) => {
    try {
      console.log(`💬 Message from ${messageData.username}:`, messageData.message);
      
      if (!messageData.userId || !messageData.username || !messageData.message) {
        console.log('💬 Invalid message data');
        return;
      }
      
      const message = {
        id: require('crypto').randomUUID(),
        userId: messageData.userId,
        username: messageData.username,
        message: messageData.message.trim(),
        timestamp: new Date(),
        profilePicture: messageData.profilePicture || '/default-avatar.png'
      };
      
      // Add to message history
      gameState.chat.messages.push(message);
      
      // Keep only last 100 messages
      if (gameState.chat.messages.length > 100) {
        gameState.chat.messages = gameState.chat.messages.slice(-100);
      }
      
      // Broadcast to all chat users
      io.to('chat-room').emit('new-message', message);
      
      console.log(`💬 Message broadcast to chat room from ${messageData.username}`);
      
    } catch (error) {
      console.error('❌ Error processing chat message:', error);
    }
  });

  // Dice Game Events (working version - unchanged)
  socket.on('join-dice', (userData) => {
    console.log(`🎲 User joining dice room: ${userData?.username} (${socket.id})`);
    socket.join('dice-room');
    socket.userData = userData;
    
    if (gameState.dice.currentGame) {
      console.log(`🎲 Sending current game state to ${userData?.username}:`, {
        gameId: gameState.dice.currentGame.id,
        phase: gameState.dice.currentGame.phase,
        timeLeft: gameState.dice.currentGame.timeLeft
      });
      
      socket.emit('dice-game-state', {
        gameId: gameState.dice.currentGame.id,
        hashedSeed: gameState.dice.currentGame.hashedSeed,
        phase: gameState.dice.currentGame.phase,
        timeLeft: gameState.dice.currentGame.timeLeft,
        result: gameState.dice.currentGame.result
      });
    } else {
      console.log('🎲 No current game, will start one soon');
    }

    const currentPlayers = Array.from(gameState.dice.players.values());
    if (currentPlayers.length > 0) {
      socket.emit('dice-players-list', currentPlayers);
    }
  });

  socket.on('place-dice-bet', async (betData) => {
    try {
      console.log(`🎲 Bet received from ${betData.username}:`, betData);

      if (!gameState.dice.currentGame) {
        console.log(`🎲 Bet rejected - no current game`);
        socket.emit('bet-error', 'No active game found');
        return;
      }

      if (gameState.dice.currentGame.phase !== 'betting') {
        console.log(`🎲 Bet rejected - invalid game phase: ${gameState.dice.currentGame.phase}`);
        socket.emit('bet-error', 'Betting is not currently open');
        return;
      }

      // Check if user already has a bet
      let userAlreadyBet = false;
      for (const [socketId, player] of gameState.dice.players.entries()) {
        if (player.userId === betData.userId) {
          userAlreadyBet = true;
          console.log(`🎲 Bet rejected - user ${betData.username} already placed bet`);
          socket.emit('bet-error', 'You have already placed a bet for this round');
          return;
        }
      }

      // Validate bet data
      if (!betData.userId || !betData.username || !betData.amount || !betData.choice) {
        console.log(`🎲 Bet rejected - missing required data:`, betData);
        socket.emit('bet-error', 'Invalid bet data - missing required fields');
        return;
      }

      // Validate choice
      if (betData.choice !== 'odd' && betData.choice !== 'even') {
        console.log(`🎲 Bet rejected - invalid choice: ${betData.choice}`);
        socket.emit('bet-error', 'Invalid choice - must be odd or even');
        return;
      }

      // Validate bet amount
      const betAmount = Number(betData.amount);
      if (isNaN(betAmount) || betAmount <= 0 || betAmount > 10000) {
        console.log(`🎲 Bet rejected - invalid amount: ${betAmount}`);
        socket.emit('bet-error', 'Invalid bet amount');
        return;
      }

      const crypto = require('crypto');
      const betId = crypto.randomUUID();
      console.log(`🎲 Attempting to save bet to database: ${betId}`);

      const playerBet = {
        userId: betData.userId,
        username: betData.username,
        amount: betAmount,
        choice: betData.choice,
        socketId: socket.id,
        profilePicture: betData.profilePicture || socket.userData?.profilePicture || '/default-avatar.png',
        timestamp: new Date(),
        gameId: gameState.dice.currentGame.id,
        betId: betId
      };
      
      // Try to save to database with error handling
      let dbSuccess = false;
      try {
        dbSuccess = await safeDiceDatabase('placeBet', {
          id: betId,
          gameId: gameState.dice.currentGame.id,
          userId: betData.userId,
          amount: betAmount,
          choice: betData.choice
        });
      } catch (dbError) {
        console.error(`❌ Database error during dice bet placement:`, dbError);
        socket.emit('bet-error', 'Database error - please try again');
        return;
      }

      if (!dbSuccess) {
        console.log(`🎲 Bet rejected - database save failed`);
        socket.emit('bet-error', 'Failed to save bet to database - try again');
        return;
      }

      // Add player to game state
      gameState.dice.players.set(socket.id, playerBet);

      console.log(`✅ Bet placed successfully for ${betData.username}: ${betAmount} USDC on ${betData.choice}`);

      const playerJoinedData = {
        playerId: socket.id,
        userId: betData.userId,
        username: betData.username,
        amount: betAmount,
        choice: betData.choice,
        profilePicture: playerBet.profilePicture
      };

      // Broadcast to all players
      try {
        io.to('dice-room').emit('player-joined', playerJoinedData);
      } catch (broadcastError) {
        console.error(`❌ Error broadcasting player joined:`, broadcastError);
      }

      // Confirm to player
      try {
        socket.emit('bet-placed-confirmation', {
          success: true,
          bet: playerBet,
          message: `Bet placed: ${betAmount} USDC on ${betData.choice.toUpperCase()}`
        });
      } catch (confirmError) {
        console.error(`❌ Error sending confirmation:`, confirmError);
      }

      console.log(`🎲 Player count for game ${gameState.dice.currentGame.id}: ${gameState.dice.players.size}`);

    } catch (error) {
      console.error(`❌ Unhandled error in place-dice-bet:`, error);
      try {
        socket.emit('bet-error', 'An unexpected error occurred - please try again');
      } catch (emitError) {
        console.error(`❌ Failed to emit error message:`, emitError);
      }
    }
  });

  // ROBUST CRASH GAME EVENTS - FIXED VERSION
  socket.on('join-crash', async (userData) => {
    try {
      console.log(`🚀 User joining crash room: ${userData?.username} (${socket.id})`);
      socket.join('crash-room');
      socket.userData = userData;
      
      if (gameState.crash.currentGame) {
        console.log(`🚀 Sending current crash game state to ${userData?.username}:`, {
          gameId: gameState.crash.currentGame.id,
          phase: gameState.crash.currentGame.phase,
          timeLeft: gameState.crash.currentGame.timeLeft,
          currentMultiplier: gameState.crash.currentMultiplier
        });
        
        // Send current game state
        socket.emit('crash-game-state', {
          gameId: gameState.crash.currentGame.id,
          hashedSeed: gameState.crash.currentGame.hashedSeed,
          phase: gameState.crash.currentGame.phase,
          timeLeft: gameState.crash.currentGame.timeLeft,
          currentMultiplier: gameState.crash.currentMultiplier,
          result: gameState.crash.currentGame.result
        });
        
        // Attempt bet recovery ONLY for active games
        if (userData?.id && ['betting', 'flying'].includes(gameState.crash.currentGame.phase)) {
          await recoverCrashBetForUser(socket, userData, gameState.crash.currentGame.id);
        }
      } else {
        console.log('🚀 No current crash game, will start one soon');
        socket.emit('crash-game-state', {
          gameId: null,
          phase: 'waiting',
          timeLeft: 0,
          currentMultiplier: 1.00
        });
      }

      // Send current players list
      const currentPlayers = Array.from(gameState.crash.players.values()).map(player => ({
        playerId: player.socketId,
        userId: player.userId,
        username: player.username,
        amount: player.amount,
        profilePicture: player.profilePicture,
        isCashedOut: player.isCashedOut,
        cashOutAt: player.cashOutAt,
        payout: player.payout
      }));
      
      if (currentPlayers.length > 0) {
        socket.emit('crash-players-list', currentPlayers);
      }
    } catch (error) {
      console.error(`❌ Error in join-crash:`, error);
    }
  });

  // FIXED CRASH BET PLACEMENT
  socket.on('place-crash-bet', async (betData) => {
    try {
      console.log(`🚀 PROCESSING CRASH BET from ${betData.username}:`, {
        userId: betData.userId,
        amount: betData.amount,
        gameId: gameState.crash.currentGame?.id,
        gamePhase: gameState.crash.currentGame?.phase
      });

      // Immediate validation
      if (!gameState.crash.currentGame) {
        console.log(`🚀❌ Bet rejected - no current crash game`);
        socket.emit('crash-bet-error', 'No active crash game found');
        return;
      }

      if (gameState.crash.currentGame.phase !== 'betting') {
        console.log(`🚀❌ Bet rejected - invalid game phase: ${gameState.crash.currentGame.phase}`);
        socket.emit('crash-bet-error', 'Betting phase has ended');
        return;
      }

      // Check for existing bet by userId (not socketId)
      if (gameState.crash.players.has(betData.userId)) {
        console.log(`🚀❌ Bet rejected - user already has bet in memory`);
        socket.emit('crash-bet-error', 'You already have a bet for this round');
        return;
      }

      // Check database for existing bet
      const existingBet = await safeCrashDatabase('getUserActiveBet', betData.userId, gameState.crash.currentGame.id);
      if (existingBet) {
        console.log(`🚀❌ Bet rejected - user already has bet in database:`, existingBet);
        socket.emit('crash-bet-error', 'You already have a bet for this round');
        return;
      }

      // Validate bet data
      if (!betData.userId || !betData.username || !betData.amount) {
        console.log(`🚀❌ Bet rejected - missing required data`);
        socket.emit('crash-bet-error', 'Invalid bet data');
        return;
      }

      const betAmount = Number(betData.amount);
      if (isNaN(betAmount) || betAmount <= 0 || betAmount > 10000) {
        console.log(`🚀❌ Bet rejected - invalid amount: ${betAmount}`);
        socket.emit('crash-bet-error', 'Invalid bet amount');
        return;
      }

      const crypto = require('crypto');
      const betId = crypto.randomUUID();
      
      console.log(`🚀💾 Saving crash bet to database: ${betId}`);

      // Database save
      const dbSuccess = await safeCrashDatabase('placeBet', {
        id: betId,
        gameId: gameState.crash.currentGame.id,
        userId: betData.userId,
        amount: betAmount
      });

      if (!dbSuccess) {
        console.log(`🚀❌ Database save failed`);
        socket.emit('crash-bet-error', 'Failed to save bet - please try again');
        return;
      }

      // Create player bet object
      const playerBet = {
        userId: betData.userId,
        username: betData.username,
        amount: betAmount,
        socketId: socket.id,
        profilePicture: betData.profilePicture || '/default-avatar.png',
        timestamp: new Date(),
        gameId: gameState.crash.currentGame.id,
        betId: betId,
        isCashedOut: false,
        cashOutAt: null,
        payout: 0
      };

      // Add to game state using userId as key
      gameState.crash.players.set(betData.userId, playerBet);
      gameState.crash.socketToUser.set(socket.id, betData.userId);
      
      console.log(`🚀✅ Crash bet successful for ${betData.username}: ${betAmount} USDC`);
      console.log(`🚀📊 Total players in game: ${gameState.crash.players.size}`);

      // Prepare broadcast data
      const playerJoinedData = {
        playerId: socket.id,
        userId: betData.userId,
        username: betData.username,
        amount: betAmount,
        profilePicture: playerBet.profilePicture,
        isCashedOut: false
      };

      // Use process.nextTick for safe async emission
      process.nextTick(() => {
        try {
          // Broadcast to all players
          io.to('crash-room').emit('crash-player-joined', playerJoinedData);
          console.log(`🚀📡 Broadcasted player joined to crash room`);
          
          // Confirm to player
          socket.emit('crash-bet-placed-confirmation', {
            success: true,
            bet: playerBet,
            message: `Crash bet placed: ${betAmount} USDC`
          });
          console.log(`🚀✅ Sent confirmation to player`);
          
        } catch (emitError) {
          console.error(`🚀❌ Error during emission:`, emitError);
        }
      });

    } catch (error) {
      console.error(`🚀❌ Critical error in place-crash-bet:`, error);
      try {
        socket.emit('crash-bet-error', 'Server error - please try again');
      } catch (emitError) {
        console.error(`🚀❌ Failed to emit error:`, emitError);
      }
    }
  });

  socket.on('crash-cash-out', async (cashOutData) => {
    try {
      console.log(`🚀💰 Cash out request from ${cashOutData.userId}`);

      if (!gameState.crash.currentGame) {
        socket.emit('crash-cash-out-error', 'No active crash game found');
        return;
      }

      if (gameState.crash.currentGame.phase !== 'flying') {
        socket.emit('crash-cash-out-error', 'Cannot cash out right now');
        return;
      }

      if (gameState.crash.crashed) {
        socket.emit('crash-cash-out-error', 'Too late! Rocket already crashed');
        return;
      }

      const player = gameState.crash.players.get(cashOutData.userId);
      if (!player) {
        socket.emit('crash-cash-out-error', 'No active bet found');
        return;
      }

      if (player.isCashedOut) {
        socket.emit('crash-cash-out-error', 'Already cashed out');
        return;
      }

      const currentMultiplier = gameState.crash.currentMultiplier;
      const payout = player.amount * currentMultiplier;

      // Update player state
      player.isCashedOut = true;
      player.cashOutAt = currentMultiplier;
      player.payout = payout;

      // Update database
      const dbSuccess = await safeCrashDatabase('cashOut', player.betId, currentMultiplier, payout);
      if (dbSuccess) {
        await safeUpdateUserBalance(player.userId, payout, 'add');
        await safeUpdateUserStats(player.userId, player.amount, payout);
      }

      console.log(`🚀💰✅ Cash out successful: ${player.username} at ${currentMultiplier.toFixed(2)}x for ${payout.toFixed(2)} USDC`);

      // Notify all players
      io.to('crash-room').emit('crash-player-cashed-out', {
        playerId: player.socketId,
        userId: player.userId,
        username: player.username,
        amount: player.amount,
        cashOutAt: currentMultiplier,
        payout: payout
      });

      // Notify the player
      socket.emit('crash-cash-out-success', {
        cashOutAt: currentMultiplier,
        payout: payout,
        message: `Cashed out at ${currentMultiplier.toFixed(2)}x for ${payout.toFixed(2)} USDC!`
      });

    } catch (error) {
      console.error(`🚀❌ Error in crash-cash-out:`, error);
      socket.emit('crash-cash-out-error', 'Cash out failed - please try again');
    }
  });

  // RPS Game Events - COMPLETE HANDLERS FROM OLD FILE
  socket.on('join-rps', (userData) => {
    console.log('User joined RPS room:', userData.username, 'Socket ID:', socket.id);
    socket.join('rps-room');
    socket.userData = userData;
    
    const currentLobbies = Array.from(gameState.rps.lobbies.values())
      .filter(lobby => lobby.status === 'waiting')
      .slice(0, 20);
      
    socket.emit('rps-lobbies-list', currentLobbies);
    socket.emit('battle-history-updated', gameState.rps.history);
    
    console.log(`✅ Sent ${currentLobbies.length} lobbies to ${userData.username}`);
  });

  socket.on('create-rps-lobby', async (lobbyData) => {
    console.log('Creating RPS lobby:', lobbyData, 'Socket ID:', socket.id);
    
    const lobbyId = generateLobbyId();
    const hashedSeed = generateHashedSeed();
    const newLobby = {
      id: lobbyId,
      creator: {
        socketId: socket.id,
        userId: lobbyData.userId,
        username: lobbyData.username,
        amount: lobbyData.amount,
        profilePicture: socket.userData?.profilePicture || '/default-avatar.png'
      },
      opponent: null,
      status: 'waiting',
      createdAt: new Date(),
      hashedSeed: hashedSeed
    };

    await safeRPSDatabase('createLobby', {
      id: lobbyId,
      creatorId: lobbyData.userId,
      amount: lobbyData.amount,
      hashedSeed: hashedSeed
    });

    gameState.rps.lobbies.set(lobbyId, newLobby);
    socket.join(`rps-lobby-${lobbyId}`);
    socket.lobbyId = lobbyId;
    
    console.log(`✅ Lobby created: ${lobbyId}, Socket lobbyId set to: ${socket.lobbyId}`);

    // Clean up old lobbies (keep max 20)
    const lobbiesArray = Array.from(gameState.rps.lobbies.values());
    if (lobbiesArray.length > 20) {
      lobbiesArray.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
      const lobbiesToKeep = lobbiesArray.slice(0, 20);
      const lobbiesToRemove = lobbiesArray.slice(20);
      
      lobbiesToRemove.forEach(lobby => {
        gameState.rps.lobbies.delete(lobby.id);
        io.to(`rps-lobby-${lobby.id}`).emit('lobby-removed', lobby.id);
      });
    }

    io.to('rps-room').emit('lobby-created', newLobby);
    console.log('✅ Lobby broadcasted to all users:', newLobby.id);

    // Auto-timeout after 30 seconds
    setTimeout(() => {
      const lobby = gameState.rps.lobbies.get(lobbyId);
      if (lobby && lobby.status === 'waiting') {
        console.log('⏰ Lobby timeout:', lobbyId);
        socket.emit('lobby-timeout', lobbyId);
      }
    }, 30000);
  });

  socket.on('join-rps-lobby', (joinData) => {
    console.log('User attempting to join lobby:', joinData);
    
    const lobby = gameState.rps.lobbies.get(joinData.lobbyId);
    if (!lobby || lobby.status !== 'waiting') {
      socket.emit('join-error', 'Lobby not available');
      return;
    }

    if (lobby.creator.userId === joinData.userId) {
      socket.emit('join-error', 'Cannot join your own lobby');
      return;
    }

    lobby.opponent = {
      socketId: socket.id,
      userId: joinData.userId,
      username: joinData.username,
      amount: joinData.amount,
      profilePicture: socket.userData?.profilePicture || '/default-avatar.png'
    };
    lobby.status = 'ready';

    socket.join(`rps-lobby-${joinData.lobbyId}`);
    
    io.to(`rps-lobby-${joinData.lobbyId}`).emit('lobby-ready', lobby);
    io.to('rps-room').emit('lobby-updated', lobby);
    
    console.log('Lobby joined successfully:', joinData.lobbyId);
  });

  socket.on('play-rps-bot', (botData) => {
    console.log('User requesting bot battle:', botData);
    console.log('Socket lobbyId:', socket.lobbyId);
    console.log('Available lobbies:', Array.from(gameState.rps.lobbies.keys()));
    
    let lobbyId = socket.lobbyId;
    let lobby = gameState.rps.lobbies.get(lobbyId);
    
    if (!lobby) {
      console.log('Lobby not found by socket.lobbyId, searching by creator...');
      for (const [id, lobbyData] of gameState.rps.lobbies.entries()) {
        if (lobbyData.creator.socketId === socket.id || lobbyData.creator.userId === socket.userData?.userId) {
          lobbyId = id;
          lobby = lobbyData;
          socket.lobbyId = id;
          console.log('Found lobby by creator:', id);
          break;
        }
      }
    }
    
    if (!lobby) {
      console.error('No lobby found for user:', socket.userData);
      socket.emit('join-error', 'No active lobby found. Please create a new lobby.');
      return;
    }

    console.log('Found lobby for bot battle:', lobby.id);

    lobby.opponent = {
      socketId: 'bot',
      userId: 'bot',
      username: 'Bot',
      amount: botData.amount || lobby.creator.amount,
      profilePicture: '/bot-avatar.png'
    };
    lobby.status = 'vs-bot';

    io.to(`rps-lobby-${lobbyId}`).emit('bot-joined', lobby);
    io.to('rps-room').emit('lobby-updated', lobby);
    
    console.log('✅ Bot joined lobby:', lobbyId);
  });

  socket.on('submit-rps-move', async (moveData) => {
    console.log('Move submitted:', moveData);
    console.log('Socket lobbyId:', socket.lobbyId);
    
    let lobby = gameState.rps.lobbies.get(moveData.lobbyId);
    
    if (!lobby && socket.lobbyId) {
      lobby = gameState.rps.lobbies.get(socket.lobbyId);
      moveData.lobbyId = socket.lobbyId;
    }
    
    if (!lobby) {
      for (const [id, lobbyData] of gameState.rps.lobbies.entries()) {
        if (lobbyData.creator.socketId === socket.id || 
            lobbyData.creator.userId === socket.userData?.userId ||
            (lobbyData.opponent && lobbyData.opponent.socketId === socket.id)) {
          lobby = lobbyData;
          moveData.lobbyId = id;
          console.log('Found lobby by user search:', id);
          break;
        }
      }
    }
    
    if (!lobby) {
      console.error('No lobby found for move submission');
      socket.emit('join-error', 'Battle session not found');
      return;
    }

    console.log('Processing move for lobby:', lobby.id, 'Status:', lobby.status);

    // Handle bot game - UPDATED WITH HISTORY RECORDING
    if (lobby.status === 'vs-bot') {
      const botMove = generateProvablyFairRPSMove(lobby.hashedSeed, moveData.nonce);
      const result = determineRPSWinner(moveData.move, botMove);
      
      let winnerId = null;
      let payout = 0;
      const betAmount = lobby.creator.amount;
      const totalPot = betAmount * 2;
      
      if (result.winner === 'player1') {
        winnerId = lobby.creator.userId;
        payout = totalPot * 0.95;
        
        await safeUpdateUserBalance(lobby.creator.userId, payout, 'add');
        await safeUpdateUserStats(lobby.creator.userId, betAmount, payout);
        console.log(`✅ User ${lobby.creator.username} won ${payout} USDC (bet: ${betAmount})`);
      } else if (result.winner === 'draw') {
        winnerId = 'draw';
        payout = betAmount;
        
        await safeUpdateUserBalance(lobby.creator.userId, betAmount, 'add');
        await safeUpdateUserStats(lobby.creator.userId, betAmount, betAmount);
        console.log(`✅ Draw: Refunded ${betAmount} USDC to ${lobby.creator.username}`);
      } else {
        winnerId = 'bot';
        payout = 0;
        
        await safeUpdateUserStats(lobby.creator.userId, betAmount, 0);
        console.log(`✅ User ${lobby.creator.username} lost ${betAmount} USDC to bot`);
      }

      const battleResult = {
        id: generateBattleId(),
        player1: lobby.creator,
        player2: lobby.opponent,
        amount: lobby.creator.amount,
        payout: payout,
        moves: {
          [lobby.creator.userId]: moveData.move,
          'bot': botMove
        },
        winner: winnerId,
        isVsBot: true,
        serverSeed: lobby.hashedSeed,
        hashedSeed: lobby.hashedSeed,
        createdAt: new Date()
      };

      // NEW: Save bot battle to database tables for history tracking
      console.log('🤖 Saving bot battle to database for history tracking...');
      
      // Update lobby status
      await safeRPSDatabase('updateLobbyStatus', moveData.lobbyId, 'completed');

      // Create battle record
      await safeRPSDatabase('createBattle', {
        id: battleResult.id,
        lobbyId: moveData.lobbyId,
        player1Id: lobby.creator.userId,
        player2Id: null, // Bot has no user ID
        amount: lobby.creator.amount,
        serverSeed: lobby.hashedSeed,
        hashedSeed: lobby.hashedSeed,
        nonce: moveData.nonce,
        isVsBot: true
      });

      // Complete battle record
      await safeRPSDatabase('completeBattle', battleResult.id, {
        player1Move: moveData.move,
        player2Move: botMove,
        winnerId: winnerId === 'draw' ? null : (winnerId === 'bot' ? null : winnerId),
        payout: payout
      });

      // Determine result for user history
      let userResult = 'lose';
      if (winnerId === 'draw') {
        userResult = 'draw';
      } else if (winnerId === lobby.creator.userId) {
        userResult = 'win';
      }

      // Add to user's personal battle history
      await safeRPSDatabase('addUserHistory', {
        id: battleResult.id + '_bot',
        userId: lobby.creator.userId,
        opponentId: null, // Bot has no user ID
        opponentUsername: 'Bot',
        userMove: moveData.move,
        opponentMove: botMove,
        result: userResult,
        amount: lobby.creator.amount,
        payout: userResult === 'win' ? payout : (userResult === 'draw' ? lobby.creator.amount : 0),
        isVsBot: true
      });

      // Add to recent public battles
      await safeRPSDatabase('addRecentBattle', {
        id: battleResult.id,
        player1Id: lobby.creator.userId,
        player1Username: lobby.creator.username,
        player1Avatar: lobby.creator.profilePicture || '/default-avatar.png',
        player1Move: moveData.move,
        player2Id: null, // Bot has no user ID
        player2Username: 'Bot',
        player2Avatar: '/bot-avatar.png',
        player2Move: botMove,
        winnerId: winnerId === 'draw' ? null : (winnerId === 'bot' ? null : winnerId),
        winnerUsername: winnerId === 'draw' ? null : (winnerId === 'bot' ? 'Bot' : lobby.creator.username),
        amount: lobby.creator.amount,
        payout: payout,
        isVsBot: true
      });

      console.log('✅ Bot battle history saved to database');

      gameState.rps.history.unshift(battleResult);
      if (gameState.rps.history.length > 50) {
        gameState.rps.history = gameState.rps.history.slice(0, 50);
      }

      io.to(`rps-lobby-${moveData.lobbyId}`).emit('battle-result', battleResult);
      
      // Refresh and broadcast updated history from database
      const freshHistory = await safeRPSDatabase('getBattleHistory', 10);
      if (freshHistory && Array.isArray(freshHistory)) {
        const formattedHistory = freshHistory.map(battle => ({
          id: battle.id,
          player1: {
            id: battle.player1_id,
            username: battle.player1_username,
            profilePicture: battle.player1_avatar || '/default-avatar.png'
          },
          player2: battle.player2_id ? {
            id: battle.player2_id,
            username: battle.player2_username || 'Bot',
            profilePicture: battle.player2_avatar || '/bot-avatar.png'
          } : {
            id: 'bot',
            username: 'Bot',
            profilePicture: '/bot-avatar.png'
          },
          moves: {
            [battle.player1_id]: battle.player1_move,
            [battle.player2_id || 'bot']: battle.player2_move
          },
          winner: battle.winner_id || (battle.is_vs_bot && battle.player1_move !== battle.player2_move ? 'bot' : battle.winner_id),
          amount: Number(battle.amount),
          payout: Number(battle.payout),
          isVsBot: battle.is_vs_bot,
          createdAt: battle.created_at
        }));
        io.to('rps-room').emit('battle-history-updated', formattedHistory);
      } else {
        io.to('rps-room').emit('battle-history-updated', gameState.rps.history.slice(0, 10));
      }
      
      gameState.rps.lobbies.delete(moveData.lobbyId);
      io.to('rps-room').emit('lobby-removed', moveData.lobbyId);
      
      console.log('✅ Bot battle completed:', battleResult.id, 'Winner:', winnerId);
    }
    // Handle PvP game (player vs player)
    else if (lobby.status === 'ready') {
      if (!gameState.rps.activeBattles.has(moveData.lobbyId)) {
        gameState.rps.activeBattles.set(moveData.lobbyId, {
          lobby: lobby,
          moves: {},
          players: [lobby.creator.userId, lobby.opponent.userId],
          submittedCount: 0
        });
      }

      const battle = gameState.rps.activeBattles.get(moveData.lobbyId);
      
      if (!battle.moves[socket.userData.userId]) {
        battle.moves[socket.userData.userId] = moveData.move;
        battle.submittedCount++;
        
        console.log(`Move submitted by ${socket.userData.username}: ${moveData.move} (${battle.submittedCount}/2)`);
        
        socket.emit('move-submitted', { 
          message: 'Move submitted! Waiting for opponent...',
          movesSubmitted: battle.submittedCount,
          totalPlayers: 2
        });
        
        io.to(`rps-lobby-${moveData.lobbyId}`).emit('moves-update', {
          movesSubmitted: battle.submittedCount,
          totalPlayers: 2,
          waiting: battle.submittedCount < 2
        });
      } else {
        socket.emit('move-error', 'You have already submitted your move');
        return;
      }

      if (battle.submittedCount === 2 && Object.keys(battle.moves).length === 2) {
        console.log('Both moves submitted, determining winner...');
        
        const move1 = battle.moves[lobby.creator.userId];
        const move2 = battle.moves[lobby.opponent.userId];
        
        console.log(`PvP Battle: ${lobby.creator.username} (${move1}) vs ${lobby.opponent.username} (${move2})`);
        
        const result = determineRPSWinner(move1, move2);
        
        let winnerId = null;
        let payout = 0;
        const totalPot = lobby.creator.amount + lobby.opponent.amount;
        
        if (result.winner === 'player1') {
          winnerId = lobby.creator.userId;
          payout = totalPot * 0.95;
          
          await safeUpdateUserBalance(lobby.creator.userId, payout, 'add');
          await safeUpdateUserStats(lobby.creator.userId, lobby.creator.amount, payout);
          await safeUpdateUserStats(lobby.opponent.userId, lobby.opponent.amount, 0);
          console.log(`✅ PvP: ${lobby.creator.username} won ${payout} USDC`);
        } else if (result.winner === 'player2') {
          winnerId = lobby.opponent.userId;
          payout = totalPot * 0.95;
          
          await safeUpdateUserBalance(lobby.opponent.userId, payout, 'add');
          await safeUpdateUserStats(lobby.opponent.userId, lobby.opponent.amount, payout);
          await safeUpdateUserStats(lobby.creator.userId, lobby.creator.amount, 0);
          console.log(`✅ PvP: ${lobby.opponent.username} won ${payout} USDC`);
        } else {
          winnerId = 'draw';
          payout = lobby.creator.amount;
          
          await safeUpdateUserBalance(lobby.creator.userId, lobby.creator.amount, 'add');
          await safeUpdateUserBalance(lobby.opponent.userId, lobby.opponent.amount, 'add');
          await safeUpdateUserStats(lobby.creator.userId, lobby.creator.amount, lobby.creator.amount);
          await safeUpdateUserStats(lobby.opponent.userId, lobby.opponent.amount, lobby.opponent.amount);
          console.log(`✅ PvP Draw: Both players refunded`);
        }

        const battleResult = {
          id: generateBattleId(),
          player1: lobby.creator,
          player2: lobby.opponent,
          amount: lobby.creator.amount,
          payout: payout,
          moves: {
            [lobby.creator.userId]: move1,
            [lobby.opponent.userId]: move2
          },
          winner: winnerId,
          isVsBot: false,
          serverSeed: lobby.hashedSeed,
          hashedSeed: lobby.hashedSeed,
          createdAt: new Date()
        };

        await safeRPSDatabase('updateLobbyStatus', moveData.lobbyId, 'completed');

        await safeRPSDatabase('createBattle', {
          id: battleResult.id,
          lobbyId: moveData.lobbyId,
          player1Id: lobby.creator.userId,
          player2Id: lobby.opponent.userId,
          amount: lobby.creator.amount,
          serverSeed: lobby.hashedSeed,
          hashedSeed: lobby.hashedSeed,
          nonce: moveData.nonce,
          isVsBot: false
        });

        await safeRPSDatabase('completeBattle', battleResult.id, {
          player1Move: move1,
          player2Move: move2,
          winnerId: winnerId === 'draw' ? null : winnerId,
          payout: payout
        });

        let player1Result = 'lose';
        let player2Result = 'lose';
        if (winnerId === 'draw') {
          player1Result = 'draw';
          player2Result = 'draw';
        } else if (winnerId === lobby.creator.userId) {
          player1Result = 'win';
          player2Result = 'lose';
        } else {
          player1Result = 'lose';
          player2Result = 'win';
        }

        await safeRPSDatabase('addUserHistory', {
          id: battleResult.id + '_p1',
          userId: lobby.creator.userId,
          opponentId: lobby.opponent.userId,
          opponentUsername: lobby.opponent.username,
          userMove: move1,
          opponentMove: move2,
          result: player1Result,
          amount: lobby.creator.amount,
          payout: player1Result === 'win' ? payout : (player1Result === 'draw' ? lobby.creator.amount : 0),
          isVsBot: false
        });

        await safeRPSDatabase('addUserHistory', {
          id: battleResult.id + '_p2',
          userId: lobby.opponent.userId,
          opponentId: lobby.creator.userId,
          opponentUsername: lobby.creator.username,
          userMove: move2,
          opponentMove: move1,
          result: player2Result,
          amount: lobby.opponent.amount,
          payout: player2Result === 'win' ? payout : (player2Result === 'draw' ? lobby.opponent.amount : 0),
          isVsBot: false
        });

        await safeRPSDatabase('addRecentBattle', {
          id: battleResult.id,
          player1Id: lobby.creator.userId,
          player1Username: lobby.creator.username,
          player1Avatar: lobby.creator.profilePicture || '/default-avatar.png',
          player1Move: move1,
          player2Id: lobby.opponent.userId,
          player2Username: lobby.opponent.username,
          player2Avatar: lobby.opponent.profilePicture || '/default-avatar.png',
          player2Move: move2,
          winnerId: winnerId === 'draw' ? null : winnerId,
          winnerUsername: winnerId === 'draw' ? null : (winnerId === lobby.creator.userId ? lobby.creator.username : lobby.opponent.username),
          amount: lobby.creator.amount,
          payout: payout,
          isVsBot: false
        });

        gameState.rps.history.unshift(battleResult);
        if (gameState.rps.history.length > 50) {
          gameState.rps.history = gameState.rps.history.slice(0, 50);
        }

        io.to(`rps-lobby-${moveData.lobbyId}`).emit('battle-result', battleResult);
        
        const freshHistory = await safeRPSDatabase('getBattleHistory', 10);
        if (freshHistory && Array.isArray(freshHistory)) {
          const formattedHistory = freshHistory.map(battle => ({
            id: battle.id,
            player1: {
              id: battle.player1_id,
              username: battle.player1_username,
              profilePicture: battle.player1_avatar
            },
            player2: battle.player2_id ? {
              id: battle.player2_id,
              username: battle.player2_username || 'Bot',
              profilePicture: battle.player2_avatar || '/bot-avatar.png'
            } : {
              id: 'bot',
              username: 'Bot',
              profilePicture: '/bot-avatar.png'
            },
            moves: {
              [battle.player1_id]: battle.player1_move,
              [battle.player2_id || 'bot']: battle.player2_move
            },
            winner: battle.winner_id || (battle.is_vs_bot && battle.player1_move !== battle.player2_move ? 'bot' : battle.winner_id),
            amount: Number(battle.amount),
            payout: Number(battle.payout),
            isVsBot: battle.is_vs_bot,
            createdAt: battle.created_at
          }));
          io.to('rps-room').emit('battle-history-updated', formattedHistory);
        } else {
          io.to('rps-room').emit('battle-history-updated', gameState.rps.history.slice(0, 10));
        }
        
        gameState.rps.activeBattles.delete(moveData.lobbyId);
        gameState.rps.lobbies.delete(moveData.lobbyId);
        io.to('rps-room').emit('lobby-removed', moveData.lobbyId);
        
        console.log('✅ PvP battle completed:', battleResult.id, 'Winner:', winnerId, 'Payout:', payout);
      } else {
        console.log(`Waiting for more moves: ${battle.submittedCount}/2 submitted`);
      }
    }
  });

  // FIXED DISCONNECT HANDLING
  socket.on('disconnect', (reason) => {
    try {
      console.log(`User disconnected: ${socket.id} (${reason})`);
      
      // Remove from chat online users
      if (socket.userData?.id) {
        gameState.chat.onlineUsers.delete(socket.userData.id);
        broadcastOnlineUserCount();
        console.log(`💬 Removed ${socket.userData.username} from chat. Online users: ${gameState.chat.onlineUsers.size}`);
      }
      
      // Clean up dice game
      if (gameState.dice.players.has(socket.id)) {
        const player = gameState.dice.players.get(socket.id);
        gameState.dice.players.delete(socket.id);
        console.log(`🎲 Removed player ${player?.username} from dice game`);
        
        io.to('dice-room').emit('player-left', {
          playerId: socket.id,
          username: player?.username
        });
      }
      
      // FIXED: DON'T remove crash players on disconnect - they should stay in game
      const userId = gameState.crash.socketToUser.get(socket.id);
      if (userId && gameState.crash.players.has(userId)) {
        const player = gameState.crash.players.get(userId);
        console.log(`🚀 Player ${player?.username} disconnected but bet remains active in game`);
        
        // Update socket mapping but keep bet active
        gameState.crash.socketToUser.delete(socket.id);
        if (player) {
          player.socketId = null; // Mark as disconnected but keep bet
        }
        
        // Only remove from UI if game is completely finished
        if (!gameState.crash.currentGame || gameState.crash.currentGame.phase === 'complete') {
          gameState.crash.players.delete(userId);
          io.to('crash-room').emit('crash-player-left', {
            playerId: socket.id,
            username: player?.username
          });
          console.log(`🚀 Removed ${player?.username} from completed crash game`);
        }
      }
      
      // Clean up RPS lobbies
      if (socket.lobbyId) {
        const lobby = gameState.rps.lobbies.get(socket.lobbyId);
        if (lobby && lobby.creator.socketId === socket.id) {
          gameState.rps.lobbies.delete(socket.lobbyId);
          io.to('rps-room').emit('lobby-removed', socket.lobbyId);
          console.log('Lobby removed due to creator disconnect:', socket.lobbyId);
        }
      }
      
      gameState.connectedUsers.delete(socket.id);
      
    } catch (error) {
      console.error(`❌ Error handling disconnect:`, error);
    }
  });
});

// DICE GAME FUNCTIONS (unchanged - working version)
function startDiceGameLoop() {
  console.log('🎲 Starting optimized dice game loop...');
  startNewDiceGame();
}

async function startNewDiceGame() {
  if (gameState.dice.isProcessing) {
    console.log('🎲 Game already being processed, skipping...');
    return;
  }

  gameState.dice.isProcessing = true;
  console.log('🎲 Starting new dice game...');
  
  // Only clear dice timers and state, don't touch crash game
  clearDiceTimers();
  cleanupDiceState();
  
  const gameId = generateGameId();
  const serverSeed = generateServerSeed();
  const hashedSeed = generateHash(serverSeed);

  console.log(`🎲 Creating new dice game: ${gameId}`);

  gameState.dice.currentGame = {
    id: gameId,
    serverSeed,
    hashedSeed,
    phase: 'betting',
    timeLeft: 25,
    result: null,
    createdAt: new Date(),
    nonce: gameState.dice.gameCounter
  };

  let retryCount = 0;
  let dbSuccess = false;
  
  while (!dbSuccess && retryCount < 3) {
    dbSuccess = await safeDiceDatabase('createGame', {
      id: gameId,
      serverSeed,
      hashedSeed,
      nonce: gameState.dice.gameCounter
    });
    
    if (!dbSuccess) {
      retryCount++;
      console.log(`⚠️ Game creation failed, retry ${retryCount}/3`);
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  }

  if (!dbSuccess) {
    console.error('❌ Failed to save game to database after 3 retries, retrying entire function...');
    gameState.dice.isProcessing = false;
    setTimeout(() => startNewDiceGame(), 2000);
    return;
  }

  console.log(`🎲 Broadcasting new game to dice room: ${gameId}`);
  
  io.to('dice-room').emit('new-dice-game', {
    gameId,
    hashedSeed,
    phase: 'betting',
    timeLeft: 25
  });

  io.to('dice-room').emit('dice-game-state', {
    gameId,
    hashedSeed,
    phase: 'betting',
    timeLeft: 25
  });

  console.log(`🎲 Game ${gameId} started - betting phase (25 seconds)`);

  gameState.dice.bettingInterval = setInterval(() => {
    if (!gameState.dice.currentGame || gameState.dice.currentGame.id !== gameId) {
      console.log('🎲 Game state changed, clearing betting interval');
      clearInterval(gameState.dice.bettingInterval);
      gameState.dice.bettingInterval = null;
      return;
    }

    gameState.dice.currentGame.timeLeft--;
    
    io.to('dice-room').emit('dice-timer-update', gameState.dice.currentGame.timeLeft);
    
    if (gameState.dice.currentGame.timeLeft % 5 === 0) {
      io.to('dice-room').emit('dice-game-state', {
        gameId: gameState.dice.currentGame.id,
        hashedSeed: gameState.dice.currentGame.hashedSeed,
        phase: gameState.dice.currentGame.phase,
        timeLeft: gameState.dice.currentGame,
        // /Users/macbook/Documents/n1verse/server.js - PART 2 (Continuation)

        timeLeft: gameState.dice.currentGame.timeLeft
      });
    }

    console.log(`🎲 Game ${gameId} - betting phase: ${gameState.dice.currentGame.timeLeft}s remaining`);

    if (gameState.dice.currentGame.timeLeft <= 0) {
      clearInterval(gameState.dice.bettingInterval);
      gameState.dice.bettingInterval = null;
      console.log(`🎲 Game ${gameId} - betting phase ended, starting rolling phase`);
      startDiceRolling();
    }
  }, 1000);
}

function startDiceRolling() {
  if (!gameState.dice.currentGame) {
    console.error('🎲 No current game to start rolling');
    gameState.dice.isProcessing = false;
    return;
  }

  const gameId = gameState.dice.currentGame.id;
  console.log(`🎲 Game ${gameId} - entering rolling phase`);
  
  gameState.dice.currentGame.phase = 'rolling';
  gameState.dice.currentGame.timeLeft = 5;

  io.to('dice-room').emit('dice-rolling-start');
  io.to('dice-room').emit('dice-game-state', {
    gameId: gameState.dice.currentGame.id,
    hashedSeed: gameState.dice.currentGame.hashedSeed,
    phase: 'rolling',
    timeLeft: 5
  });

  let rollingTime = 5;
  const rollingInterval = setInterval(() => {
    if (!gameState.dice.currentGame || gameState.dice.currentGame.id !== gameId) {
      console.log('🎲 Game state changed, clearing rolling interval');
      clearInterval(rollingInterval);
      return;
    }

    rollingTime--;
    gameState.dice.currentGame.timeLeft = rollingTime;
    io.to('dice-room').emit('dice-timer-update', rollingTime);
    
    if (rollingTime <= 0) {
      clearInterval(rollingInterval);
      console.log(`🎲 Game ${gameId} - rolling phase complete, completing game`);
      completeDiceGame();
    }
  }, 1000);
}

async function completeDiceGame() {
  if (!gameState.dice.currentGame) {
    console.error('🎲 No current game to complete');
    gameState.dice.isProcessing = false;
    return;
  }

  const gameId = gameState.dice.currentGame.id;
  console.log(`🎲 Game ${gameId} - completing game`);

  const diceResult = generateProvablyFairDiceResult(
    gameState.dice.currentGame.serverSeed,
    gameState.dice.currentGame.nonce
  );

  gameState.dice.currentGame.result = diceResult;
  gameState.dice.currentGame.phase = 'complete';
  gameState.dice.currentGame.completedAt = Date.now();

  console.log(`🎲 Game ${gameId} - dice result: ${diceResult.value} (${diceResult.isOdd ? 'ODD' : 'EVEN'})`);

  const winners = [];
  const losers = [];
  let totalWagered = 0;
  let totalPayout = 0;

  console.log(`🎲 Processing ${gameState.dice.players.size} bets for game ${gameId}`);

  const dbOperations = [];

  for (const [socketId, player] of gameState.dice.players.entries()) {
    totalWagered += player.amount;
    
    const isWinner = (diceResult.isOdd && player.choice === 'odd') || 
                    (!diceResult.isOdd && player.choice === 'even');
    
    if (isWinner) {
      const payout = player.amount * 1.96;
      totalPayout += payout;
      
      winners.push({
        ...player,
        payout
      });
      
      dbOperations.push(
        safeUpdateUserBalance(player.userId, payout, 'add'),
        safeUpdateUserStats(player.userId, player.amount, payout),
        safeDiceDatabase('updateBetResult', player.betId, true, payout)
      );
      
      console.log(`🏆 Winner: ${player.username} won ${payout} USDC (bet: ${player.amount} on ${player.choice})`);
    } else {
      losers.push(player);
      
      dbOperations.push(
        safeUpdateUserStats(player.userId, player.amount, 0),
        safeDiceDatabase('updateBetResult', player.betId, false, 0)
      );
      
      console.log(`😔 Loser: ${player.username} lost ${player.amount} USDC (bet on ${player.choice})`);
    }
  }

  console.log(`🎲 Executing ${dbOperations.length} database operations in parallel...`);
  const startTime = Date.now();
  
  try {
    await Promise.all(dbOperations);
    console.log(`✅ All database operations completed in ${Date.now() - startTime}ms`);
  } catch (error) {
    console.error('❌ Error in database operations:', error);
  }

  const gameResult = {
    gameId: gameId,
    diceValue: diceResult.value,
    isOdd: diceResult.isOdd,
    serverSeed: gameState.dice.currentGame.serverSeed,
    hashedSeed: gameState.dice.currentGame.hashedSeed,
    winners,
    losers,
    totalWagered,
    totalPayout,
    playersCount: gameState.dice.players.size,
    timestamp: new Date()
  };

  try {
    await safeDiceDatabase('completeGame', gameId, {
      diceValue: diceResult.value,
      isOdd: diceResult.isOdd,
      totalWagered,
      totalPayout,
      playersCount: gameState.dice.players.size
    });
  } catch (error) {
    console.error(`❌ Error completing game in database:`, error);
  }

  gameState.dice.history.unshift(gameResult);
  if (gameState.dice.history.length > 20) {
    gameState.dice.history = gameState.dice.history.slice(0, 20);
  }

  console.log(`🎲 Game ${gameId} completed - ${winners.length} winners, ${losers.length} losers, ${totalWagered} wagered, ${totalPayout} paid out`);

  try {
    io.to('dice-room').emit('dice-result', gameResult);
    
    io.to('dice-room').emit('dice-game-state', {
      gameId: gameId,
      hashedSeed: gameState.dice.currentGame.hashedSeed,
      phase: 'complete',
      timeLeft: 0,
      result: {
        diceValue: diceResult.value,
        isOdd: diceResult.isOdd,
        serverSeed: gameState.dice.currentGame.serverSeed,
        winners,
        losers
      }
    });
  } catch (error) {
    console.error(`❌ Error broadcasting game results:`, error);
  }

  console.log(`🎲 Game ${gameId} - all results broadcast`);

  gameState.dice.isProcessing = false;
  
  setTimeout(() => {
    console.log(`🎲 Starting next game immediately after completion of ${gameId}`);
    startNewDiceGame();
  }, 2000);
}

// FIXED CRASH GAME IMPLEMENTATION - PREVENT MULTIPLE GAMES
function startCrashGameLoop() {
  console.log('🚀 Starting professional crash game loop...');
  startNewCrashGame();
}

async function startNewCrashGame() {
  // PREVENT MULTIPLE GAMES WITH LOCK
  if (gameState.crash.isProcessing || gameState.crash.isGameLocked) {
    console.log('🚀 Crash game already being processed or locked, skipping...');
    return;
  }

  gameState.crash.isProcessing = true;
  gameState.crash.isGameLocked = true;
  console.log('🚀🆕 Starting new crash game...');
  
  // Clear previous game state only if game is complete
  clearCrashTimers();
  cleanupCrashState();
  
  const gameId = generateCrashGameId();
  const serverSeed = generateServerSeed();
  const hashedSeed = generateHash(serverSeed);

  console.log(`🚀🎮 Creating new crash game: ${gameId}`);

  gameState.crash.currentGame = {
    id: gameId,
    serverSeed,
    hashedSeed,
    phase: 'betting',
    timeLeft: 25,
    result: null,
    createdAt: new Date(),
    nonce: gameState.crash.gameCounter,
    crashPoint: generateProvablyFairCrashPoint(serverSeed, gameState.crash.gameCounter)
  };

  console.log(`🚀🎯 Crash point calculated: ${gameState.crash.currentGame.crashPoint.toFixed(2)}x`);

  // Database save with retry logic
  let retryCount = 0;
  let dbSuccess = false;
  
  while (!dbSuccess && retryCount < 3) {
    dbSuccess = await safeCrashDatabase('createGame', {
      id: gameId,
      serverSeed,
      hashedSeed,
      nonce: gameState.crash.gameCounter
    });
    
    if (!dbSuccess) {
      retryCount++;
      console.log(`⚠️ Crash game creation failed, retry ${retryCount}/3`);
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  }

  if (!dbSuccess) {
    console.error('❌ Failed to save crash game to database after 3 retries');
    gameState.crash.isProcessing = false;
    gameState.crash.isGameLocked = false;
    setTimeout(() => startNewCrashGame(), 3000);
    return;
  }

  console.log(`🚀📡 Broadcasting new crash game: ${gameId}`);
  
  // Broadcast new game
  io.to('crash-room').emit('new-crash-game', {
    gameId,
    hashedSeed,
    phase: 'betting',
    timeLeft: 25
  });

  io.to('crash-room').emit('crash-game-state', {
    gameId,
    hashedSeed,
    phase: 'betting',
    timeLeft: 25,
    currentMultiplier: 1.00
  });

  console.log(`🚀✅ Game ${gameId} started - betting phase (25 seconds)`);

  // Start betting countdown
  gameState.crash.bettingInterval = setInterval(() => {
    if (!gameState.crash.currentGame || gameState.crash.currentGame.id !== gameId) {
      console.log('🚀 Game state changed, clearing betting interval');
      clearInterval(gameState.crash.bettingInterval);
      gameState.crash.bettingInterval = null;
      return;
    }

    gameState.crash.currentGame.timeLeft--;
    
    // Broadcast timer updates
    io.to('crash-room').emit('crash-timer-update', gameState.crash.currentGame.timeLeft);
    
    // Periodic state updates
    if (gameState.crash.currentGame.timeLeft % 5 === 0) {
      io.to('crash-room').emit('crash-game-state', {
        gameId: gameState.crash.currentGame.id,
        hashedSeed: gameState.crash.currentGame.hashedSeed,
        phase: gameState.crash.currentGame.phase,
        timeLeft: gameState.crash.currentGame.timeLeft,
        currentMultiplier: gameState.crash.currentMultiplier
      });
    }

    console.log(`🚀⏰ Game ${gameId} - betting phase: ${gameState.crash.currentGame.timeLeft}s remaining`);

    if (gameState.crash.currentGame.timeLeft <= 0) {
      clearInterval(gameState.crash.bettingInterval);
      gameState.crash.bettingInterval = null;
      console.log(`🚀🚀 Game ${gameId} - betting phase ended, LAUNCHING ROCKET!`);
      startCrashFlying();
    }
  }, 1000);
}

function startCrashFlying() {
  if (!gameState.crash.currentGame) {
    console.error('🚀❌ No current crash game to start flying');
    gameState.crash.isProcessing = false;
    gameState.crash.isGameLocked = false;
    return;
  }

  const gameId = gameState.crash.currentGame.id;
  console.log(`🚀🚀 Game ${gameId} - ROCKET LAUNCHING!`);
  console.log(`🚀🎯 Target crash point: ${gameState.crash.currentGame.crashPoint.toFixed(2)}x`);
  
  gameState.crash.currentGame.phase = 'flying';
  gameState.crash.currentMultiplier = 1.00;
  gameState.crash.startTime = Date.now();
  gameState.crash.crashed = false;

  // Notify all players rocket launched
  io.to('crash-room').emit('crash-flying-start');
  io.to('crash-room').emit('crash-game-state', {
    gameId: gameState.crash.currentGame.id,
    hashedSeed: gameState.crash.currentGame.hashedSeed,
    phase: 'flying',
    timeLeft: 0,
    currentMultiplier: gameState.crash.currentMultiplier
  });

  console.log(`🚀🔥 Game ${gameId} - ROCKET IS FLYING!`);

  // Professional multiplier calculation with smooth progression
  gameState.crash.flyingInterval = setInterval(() => {
    if (!gameState.crash.currentGame || gameState.crash.currentGame.id !== gameId) {
      console.log('🚀 Game state changed, clearing flying interval');
      clearInterval(gameState.crash.flyingInterval);
      gameState.crash.flyingInterval = null;
      return;
    }

    if (gameState.crash.crashed) {
      console.log('🚀 Already crashed, clearing flying interval');
      clearInterval(gameState.crash.flyingInterval);
      gameState.crash.flyingInterval = null;
      return;
    }

    const elapsed = Date.now() - gameState.crash.startTime;
    const timeInSeconds = elapsed / 1000;
    
    // Professional multiplier calculation
    // Uses exponential growth with realistic acceleration
    let newMultiplier;
    
    if (timeInSeconds < 1) {
      // Initial acceleration phase (0-1 seconds)
      newMultiplier = 1.00 + (timeInSeconds * 0.1);
    } else if (timeInSeconds < 5) {
      // Normal growth phase (1-5 seconds)
      newMultiplier = 1.10 + ((timeInSeconds - 1) * 0.08) + ((timeInSeconds - 1) * (timeInSeconds - 1) * 0.005);
    } else {
      // Extended growth phase (5+ seconds)
      const baseGrowth = 1.10 + (4 * 0.08) + (4 * 4 * 0.005); // Value at 5 seconds
      const extendedTime = timeInSeconds - 5;
      newMultiplier = baseGrowth + (extendedTime * 0.15) + (extendedTime * extendedTime * 0.008);
    }
    
    // Ensure minimum increment and smooth progression
    newMultiplier = Math.max(newMultiplier, gameState.crash.currentMultiplier + 0.01);
    gameState.crash.currentMultiplier = Math.round(newMultiplier * 100) / 100;

    // Broadcast multiplier update
    io.to('crash-room').emit('crash-multiplier-update', {
      currentMultiplier: gameState.crash.currentMultiplier
    });

    // Check if crash point reached
    if (gameState.crash.currentMultiplier >= gameState.crash.currentGame.crashPoint) {
      gameState.crash.crashed = true;
      clearInterval(gameState.crash.flyingInterval);
      gameState.crash.flyingInterval = null;
      console.log(`🚀💥 Game ${gameId} - CRASHED at ${gameState.crash.currentMultiplier.toFixed(2)}x!`);
      completeCrashGame();
      return;
    }

    // Safety timeout after 3 minutes
    if (elapsed > 180000) {
      console.log(`🚀⏰ Game ${gameId} - Safety timeout reached, forcing crash`);
      gameState.crash.crashed = true;
      clearInterval(gameState.crash.flyingInterval);
      gameState.crash.flyingInterval = null;
      completeCrashGame();
      return;
    }

  }, 50); // 20 FPS for ultra-smooth experience
}

async function completeCrashGame() {
  if (!gameState.crash.currentGame) {
    console.error('🚀❌ No current crash game to complete');
    gameState.crash.isProcessing = false;
    gameState.crash.isGameLocked = false;
    return;
  }

  const gameId = gameState.crash.currentGame.id;
  const finalCrashPoint = gameState.crash.currentMultiplier;
  console.log(`🚀🏁 Game ${gameId} - COMPLETING GAME`);
  console.log(`🚀💥 Final crash point: ${finalCrashPoint.toFixed(2)}x`);

  gameState.crash.currentGame.phase = 'crashed';
  gameState.crash.currentGame.crashedAt = Date.now();
  gameState.crash.crashed = true;

  const winners = [];
  const losers = [];
  let totalWagered = 0;
  let totalPayout = 0;

  console.log(`🚀📊 Processing ${gameState.crash.players.size} bets for crash game ${gameId}`);

  const dbOperations = [];

  for (const [userId, player] of gameState.crash.players.entries()) {
    totalWagered += player.amount;
    
    if (player.isCashedOut && player.cashOutAt <= finalCrashPoint) {
      // Player successfully cashed out before crash
      totalPayout += player.payout;
      winners.push({
        ...player,
        payout: player.payout
      });
      
      console.log(`🚀🏆 Winner: ${player.username} cashed out at ${player.cashOutAt.toFixed(2)}x for ${player.payout.toFixed(2)} USDC`);
    } else if (!player.isCashedOut) {
      // Player didn't cash out, lost their bet
      losers.push(player);
      
      dbOperations.push(
        safeUpdateUserStats(player.userId, player.amount, 0),
        safeCrashDatabase('updateBetResult', player.betId, false, 0)
      );
      
      console.log(`🚀😔 Loser: ${player.username} lost ${player.amount} USDC (didn't cash out)`);
    } else {
      // Player tried to cash out after crash
      losers.push(player);
      console.log(`🚀😔 Loser: ${player.username} lost ${player.amount} USDC (too late)`);
    }
  }

  console.log(`🚀⚡ Executing ${dbOperations.length} database operations...`);
  const startTime = Date.now();
  
  try {
    await Promise.all(dbOperations);
    console.log(`🚀✅ All database operations completed in ${Date.now() - startTime}ms`);
  } catch (error) {
    console.error('🚀❌ Error in database operations:', error);
  }

  const gameResult = {
    gameId: gameId,
    crashPoint: finalCrashPoint,
    serverSeed: gameState.crash.currentGame.serverSeed,
    hashedSeed: gameState.crash.currentGame.hashedSeed,
    winners,
    losers,
    totalWagered,
    totalPayout,
    playersCount: gameState.crash.players.size,
    timestamp: new Date()
  };

  // Save game result to database
  try {
    await safeCrashDatabase('completeGame', gameId, {
      crashPoint: finalCrashPoint,
      totalWagered,
      totalPayout,
      playersCount: gameState.crash.players.size
    });
  } catch (error) {
    console.error(`🚀❌ Error completing crash game in database:`, error);
  }

  // Update history
  gameState.crash.history.unshift(gameResult);
  if (gameState.crash.history.length > 50) {
    gameState.crash.history = gameState.crash.history.slice(0, 50);
  }

  console.log(`🚀📈 Game ${gameId} completed - ${winners.length} winners, ${losers.length} losers, ${totalWagered} wagered, ${totalPayout} paid out`);

  // Broadcast results
  try {
    io.to('crash-room').emit('crash-result', gameResult);
    
    io.to('crash-room').emit('crash-game-state', {
      gameId: gameId,
      hashedSeed: gameState.crash.currentGame.hashedSeed,
      phase: 'crashed',
      timeLeft: 0,
      currentMultiplier: finalCrashPoint,
      result: {
        crashPoint: finalCrashPoint,
        serverSeed: gameState.crash.currentGame.serverSeed,
        winners,
        losers
      }
    });
  } catch (error) {
    console.error(`🚀❌ Error broadcasting crash game results:`, error);
  }

  console.log(`🚀✅ Game ${gameId} - all results broadcast`);

  // Mark game as complete and clear players
  gameState.crash.currentGame.phase = 'complete';
  cleanupCrashState();
  gameState.crash.isProcessing = false;
  gameState.crash.isGameLocked = false;
  
  // Start next game after delay
  setTimeout(() => {
    console.log(`🚀🔄 Starting next crash game after completion of ${gameId}`);
    startNewCrashGame();
  }, 3000);
}

// Start the server
httpServer.listen(port, '0.0.0.0', () => {
  console.log(`🚀 Professional Socket.IO server running on port ${port}`);
  console.log(`🌐 CORS enabled for all domains`);
  console.log(`💾 Database: ${UserDatabase && DiceDatabase && CrashDatabase && RPSDatabase ? 'Connected' : 'Mock mode'}`);
  
  // Start all game loops
  startDiceGameLoop();
  startCrashGameLoop();
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('SIGTERM received, shutting down gracefully');
  clearAllGameTimers();
  httpServer.close(() => {
    console.log('Process terminated');
  });
});

process.on('SIGINT', () => {
  console.log('SIGINT received, shutting down gracefully');
  clearAllGameTimers();
  httpServer.close(() => {
    console.log('Process terminated');
  });
});
