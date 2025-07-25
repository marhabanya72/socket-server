// /Users/macbook/Documents/n1verse/server.js
// Complete Socket.IO server for Railway deployment - WITH COMPLETELY FIXED CRASH GAME
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
  }
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

  // User Database functions
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
    }
  };

  // RPS Database functions
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
    updateBetResult: async () => { console.log('📝 Mock: updateCrashBetResult called'); return true; }
  };
  RPSDatabase = {
    createLobby: async () => { console.log('📝 Mock: createLobby called'); return true; }
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

// Game state management
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
    players: new Map(),
    gameCounter: 0,
    bettingInterval: null,
    flyingInterval: null,
    isProcessing: false,
    currentMultiplier: 1.00,
    startTime: null,
    crashed: false
  },
  rps: {
    lobbies: new Map(),
    activeBattles: new Map(),
    history: []
  },
  chat: {
    messages: []
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

function generateProvablyFairCrashPoint(serverSeed, nonce) {
  const crypto = require('crypto');
  const hmac = crypto.createHmac('sha256', serverSeed);
  hmac.update(`${nonce}:crash`);
  const hash = hmac.digest('hex');
  
  const hexSubstring = hash.substring(0, 8);
  const H = parseInt(hexSubstring, 16);
  const luckyNumber = H % 1000000;
  
  let crashPoint;
  
  if (luckyNumber < 30000) {
    crashPoint = 100;
  } else {
    crashPoint = (97 * 1000000) / (1000000 - luckyNumber);
  }
  
  const multiplier = Math.floor(crashPoint) / 100;
  return Math.max(1.00, Math.min(multiplier, 50000.00));
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

// Complete crash game cleanup
function cleanupCrashState() {
  const connectedCrashSockets = Array.from(gameState.crash.players.keys());
  gameState.crash.players.clear();
  gameState.crash.currentMultiplier = 1.00;
  gameState.crash.startTime = null;
  gameState.crash.crashed = false;
  console.log(`🧹 Cleared ${connectedCrashSockets.length} crash players`);
}

// Socket.IO event handlers
io.on('connection', (socket) => {
  console.log('User connected:', socket.id);

  socket.on('user-connect', (userData) => {
    gameState.connectedUsers.set(socket.id, userData);
    socket.userData = userData;
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

    let userAlreadyBet = false;
    for (const [socketId, player] of gameState.dice.players.entries()) {
      if (player.userId === betData.userId) {
        userAlreadyBet = true;
        console.log(`🎲 Bet rejected - user ${betData.username} already placed bet`);
        socket.emit('bet-error', 'You have already placed a bet for this round');
        return;
      }
    }

    const crypto = require('crypto');
    const betId = crypto.randomUUID();
    console.log(`🎲 Attempting to save bet to database: ${betId}`);

    const playerBet = {
      userId: betData.userId,
      username: betData.username,
      amount: betData.amount,
      choice: betData.choice,
      socketId: socket.id,
      profilePicture: betData.profilePicture || socket.userData?.profilePicture || '/default-avatar.png',
      timestamp: new Date(),
      gameId: gameState.dice.currentGame.id,
      betId: betId
    };
    
    const dbSuccess = await safeDiceDatabase('placeBet', {
      id: betId,
      gameId: gameState.dice.currentGame.id,
      userId: betData.userId,
      amount: betData.amount,
      choice: betData.choice
    });

    if (!dbSuccess) {
      console.log(`🎲 Bet rejected - database save failed`);
      socket.emit('bet-error', 'Failed to save bet to database - try again');
      return;
    }

    gameState.dice.players.set(socket.id, playerBet);

    console.log(`✅ Bet placed successfully for ${betData.username}: ${betData.amount} USDC on ${betData.choice}`);

    const playerJoinedData = {
      playerId: socket.id,
      userId: betData.userId,
      username: betData.username,
      amount: betData.amount,
      choice: betData.choice,
      profilePicture: playerBet.profilePicture
    };

    io.to('dice-room').emit('player-joined', playerJoinedData);

    socket.emit('bet-placed-confirmation', {
      success: true,
      bet: playerBet,
      message: `Bet placed: ${betData.amount} USDC on ${betData.choice.toUpperCase()}`
    });

    console.log(`🎲 Player count for game ${gameState.dice.currentGame.id}: ${gameState.dice.players.size}`);
  });

  // COMPLETELY REWRITTEN CRASH GAME EVENTS - MODELED AFTER WORKING DICE GAME
  socket.on('join-crash', (userData) => {
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
      
      socket.emit('crash-game-state', {
        gameId: gameState.crash.currentGame.id,
        hashedSeed: gameState.crash.currentGame.hashedSeed,
        phase: gameState.crash.currentGame.phase,
        timeLeft: gameState.crash.currentGame.timeLeft,
        currentMultiplier: gameState.crash.currentMultiplier,
        result: gameState.crash.currentGame.result
      });
    } else {
      console.log('🚀 No current crash game, will start one soon');
    }

    const currentPlayers = Array.from(gameState.crash.players.values());
    if (currentPlayers.length > 0) {
      socket.emit('crash-players-list', currentPlayers);
    }
  });

  socket.on('place-crash-bet', async (betData) => {
    console.log(`🚀 Crash bet received from ${betData.username}:`, betData);

    if (!gameState.crash.currentGame) {
      console.log(`🚀 Bet rejected - no current crash game`);
      socket.emit('crash-bet-error', 'No active crash game found');
      return;
    }

    if (gameState.crash.currentGame.phase !== 'betting') {
      console.log(`🚀 Bet rejected - invalid game phase: ${gameState.crash.currentGame.phase}`);
      socket.emit('crash-bet-error', 'Betting is not currently open');
      return;
    }

    let userAlreadyBet = false;
    for (const [socketId, player] of gameState.crash.players.entries()) {
      if (player.userId === betData.userId) {
        userAlreadyBet = true;
        console.log(`🚀 Bet rejected - user ${betData.username} already placed bet`);
        socket.emit('crash-bet-error', 'You have already placed a bet for this round');
        return;
      }
    }

    const crypto = require('crypto');
    const betId = crypto.randomUUID();
    console.log(`🚀 Attempting to save crash bet to database: ${betId}`);

    const playerBet = {
      userId: betData.userId,
      username: betData.username,
      amount: betData.amount,
      socketId: socket.id,
      profilePicture: betData.profilePicture || socket.userData?.profilePicture || '/default-avatar.png',
      timestamp: new Date(),
      gameId: gameState.crash.currentGame.id,
      betId: betId,
      isCashedOut: false,
      cashOutAt: null,
      payout: 0
    };
    
    const dbSuccess = await safeCrashDatabase('placeBet', {
      id: betId,
      gameId: gameState.crash.currentGame.id,
      userId: betData.userId,
      amount: betData.amount
    });

    if (!dbSuccess) {
      console.log(`🚀 Bet rejected - database save failed`);
      socket.emit('crash-bet-error', 'Failed to save bet to database - try again');
      return;
    }

    gameState.crash.players.set(socket.id, playerBet);

    console.log(`✅ Crash bet placed successfully for ${betData.username}: ${betData.amount} USDC`);

    const playerJoinedData = {
      playerId: socket.id,
      userId: betData.userId,
      username: betData.username,
      amount: betData.amount,
      profilePicture: playerBet.profilePicture,
      isCashedOut: false
    };

    io.to('crash-room').emit('crash-player-joined', playerJoinedData);

    socket.emit('crash-bet-placed-confirmation', {
      success: true,
      bet: playerBet,
      message: `Crash bet placed: ${betData.amount} USDC`
    });

    console.log(`🚀 Player count for crash game ${gameState.crash.currentGame.id}: ${gameState.crash.players.size}`);
  });

  socket.on('crash-cash-out', async (cashOutData) => {
    console.log(`🚀 Cash out request from ${cashOutData.userId}:`, cashOutData);

    if (!gameState.crash.currentGame) {
      console.log(`🚀 Cash out rejected - no current crash game`);
      socket.emit('crash-cash-out-error', 'No active crash game found');
      return;
    }

    if (gameState.crash.currentGame.phase !== 'flying') {
      console.log(`🚀 Cash out rejected - game not in flying phase: ${gameState.crash.currentGame.phase}`);
      socket.emit('crash-cash-out-error', 'Cannot cash out right now');
      return;
    }

    if (gameState.crash.crashed) {
      console.log(`🚀 Cash out rejected - rocket already crashed`);
      socket.emit('crash-cash-out-error', 'Too late! Rocket already crashed');
      return;
    }

    const player = gameState.crash.players.get(socket.id);
    if (!player) {
      console.log(`🚀 Cash out rejected - player not found`);
      socket.emit('crash-cash-out-error', 'No active bet found');
      return;
    }

    if (player.isCashedOut) {
      console.log(`🚀 Cash out rejected - already cashed out`);
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
      // Update user balance
      await safeUpdateUserBalance(player.userId, payout, 'add');
      await safeUpdateUserStats(player.userId, player.amount, payout);
    }

    console.log(`✅ Cash out successful for ${player.username}: ${payout.toFixed(2)} USDC at ${currentMultiplier.toFixed(2)}x`);

    // Notify all players about the cash out
    io.to('crash-room').emit('crash-player-cashed-out', {
      playerId: socket.id,
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
  });

  socket.on('disconnect', () => {
    console.log('User disconnected:', socket.id);
    
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
    
    // Clean up crash game
    if (gameState.crash.players.has(socket.id)) {
      const player = gameState.crash.players.get(socket.id);
      gameState.crash.players.delete(socket.id);
      console.log(`🚀 Removed player ${player?.username} from crash game`);
      
      io.to('crash-room').emit('crash-player-left', {
        playerId: socket.id,
        username: player?.username
      });
    }
    
    gameState.connectedUsers.delete(socket.id);
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

// COMPLETELY REWRITTEN CRASH GAME FUNCTIONS - MODELED AFTER WORKING DICE GAME
function startCrashGameLoop() {
  console.log('🚀 Starting crash game loop...');
  startNewCrashGame();
}

async function startNewCrashGame() {
  if (gameState.crash.isProcessing) {
    console.log('🚀 Crash game already being processed, skipping...');
    return;
  }

  gameState.crash.isProcessing = true;
  console.log('🚀 Starting new crash game...');
  
  // Clear previous game state completely
  if (gameState.crash.bettingInterval) {
    clearInterval(gameState.crash.bettingInterval);
    gameState.crash.bettingInterval = null;
  }
  if (gameState.crash.flyingInterval) {
    clearInterval(gameState.crash.flyingInterval);
    gameState.crash.flyingInterval = null;
  }
  
  gameState.crash.players.clear();
  gameState.crash.currentMultiplier = 1.00;
  gameState.crash.startTime = null;
  gameState.crash.crashed = false;
  
  const gameId = generateCrashGameId();
  const serverSeed = generateServerSeed();
  const hashedSeed = generateHash(serverSeed);

  console.log(`🚀 Creating new crash game: ${gameId}`);

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

  console.log(`🚀 Crash point calculated: ${gameState.crash.currentGame.crashPoint.toFixed(2)}x`);

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
    console.error('❌ Failed to save crash game to database after 3 retries, retrying entire function...');
    gameState.crash.isProcessing = false;
    setTimeout(() => startNewCrashGame(), 2000);
    return;
  }

  console.log(`🚀 Broadcasting new crash game to crash room: ${gameId}`);
  
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

  console.log(`🚀 Game ${gameId} started - betting phase (25 seconds)`);

  // Start betting countdown with detailed logging like dice game
  gameState.crash.bettingInterval = setInterval(() => {
    if (!gameState.crash.currentGame || gameState.crash.currentGame.id !== gameId) {
      console.log('🚀 Game state changed, clearing betting interval');
      clearInterval(gameState.crash.bettingInterval);
      gameState.crash.bettingInterval = null;
      return;
    }

    gameState.crash.currentGame.timeLeft--;
    
    io.to('crash-room').emit('crash-timer-update', gameState.crash.currentGame.timeLeft);
    
    if (gameState.crash.currentGame.timeLeft % 5 === 0) {
      io.to('crash-room').emit('crash-game-state', {
        gameId: gameState.crash.currentGame.id,
        hashedSeed: gameState.crash.currentGame.hashedSeed,
        phase: gameState.crash.currentGame.phase,
        timeLeft: gameState.crash.currentGame.timeLeft,
        currentMultiplier: gameState.crash.currentMultiplier
      });
    }

    console.log(`🚀 Game ${gameId} - betting phase: ${gameState.crash.currentGame.timeLeft}s remaining`);

    if (gameState.crash.currentGame.timeLeft <= 0) {
      clearInterval(gameState.crash.bettingInterval);
      gameState.crash.bettingInterval = null;
      console.log(`🚀 Game ${gameId} - betting phase ended, starting flying phase`);
      startCrashFlying();
    }
  }, 1000);
}

function startCrashFlying() {
  if (!gameState.crash.currentGame) {
    console.error('🚀 No current crash game to start flying');
    gameState.crash.isProcessing = false;
    return;
  }

  const gameId = gameState.crash.currentGame.id;
  console.log(`🚀 Game ${gameId} - entering flying phase`);
  console.log(`🚀 Game ${gameId} - target crash point: ${gameState.crash.currentGame.crashPoint.toFixed(2)}x`);
  
  gameState.crash.currentGame.phase = 'flying';
  gameState.crash.currentMultiplier = 1.00;
  gameState.crash.startTime = Date.now();
  gameState.crash.crashed = false;

  io.to('crash-room').emit('crash-flying-start');
  io.to('crash-room').emit('crash-game-state', {
    gameId: gameState.crash.currentGame.id,
    hashedSeed: gameState.crash.currentGame.hashedSeed,
    phase: 'flying',
    timeLeft: 0,
    currentMultiplier: gameState.crash.currentMultiplier
  });

  console.log(`🚀 Game ${gameId} - rocket launched! Flying...`);

  // Flying phase with smooth multiplier updates
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
    
    // Smooth multiplier calculation with acceleration
    let newMultiplier = 1.00 + (timeInSeconds * 0.15) + (timeInSeconds * timeInSeconds * 0.008);
    
    // Ensure consistent increments
    newMultiplier = Math.max(newMultiplier, gameState.crash.currentMultiplier + 0.01);
    gameState.crash.currentMultiplier = Math.round(newMultiplier * 100) / 100;

    // Broadcast multiplier update
    io.to('crash-room').emit('crash-multiplier-update', {
      currentMultiplier: gameState.crash.currentMultiplier
    });

    // Log every 0.5 seconds for visibility
    if (Math.floor(elapsed / 500) !== Math.floor((elapsed - 50) / 500)) {
      console.log(`🚀 Game ${gameId} - flying: ${gameState.crash.currentMultiplier.toFixed(2)}x (target: ${gameState.crash.currentGame.crashPoint.toFixed(2)}x)`);
    }

    // Check if we've reached crash point
    if (gameState.crash.currentMultiplier >= gameState.crash.currentGame.crashPoint) {
      gameState.crash.crashed = true;
      clearInterval(gameState.crash.flyingInterval);
      gameState.crash.flyingInterval = null;
      console.log(`🚀 Game ${gameId} - CRASHED at ${gameState.crash.currentMultiplier.toFixed(2)}x!`);
      completeCrashGame();
      return;
    }

    // Safety timeout after 2 minutes
    if (elapsed > 120000) {
      console.log(`🚀 Game ${gameId} - Safety timeout reached, forcing crash`);
      gameState.crash.crashed = true;
      clearInterval(gameState.crash.flyingInterval);
      gameState.crash.flyingInterval = null;
      completeCrashGame();
      return;
    }

  }, 50); // 20 FPS for smooth experience
}

async function completeCrashGame() {
  if (!gameState.crash.currentGame) {
    console.error('🚀 No current crash game to complete');
    gameState.crash.isProcessing = false;
    return;
  }

  const gameId = gameState.crash.currentGame.id;
  const finalCrashPoint = gameState.crash.currentMultiplier;
  console.log(`🚀 Game ${gameId} - completing game`);
  console.log(`🚀 Game ${gameId} - final crash point: ${finalCrashPoint.toFixed(2)}x`);

  gameState.crash.currentGame.phase = 'crashed';
  gameState.crash.currentGame.crashedAt = Date.now();
  gameState.crash.crashed = true;

  const winners = [];
  const losers = [];
  let totalWagered = 0;
  let totalPayout = 0;

  console.log(`🚀 Processing ${gameState.crash.players.size} bets for crash game ${gameId}`);

  const dbOperations = [];

  for (const [socketId, player] of gameState.crash.players.entries()) {
    totalWagered += player.amount;
    
    if (player.isCashedOut && player.cashOutAt <= finalCrashPoint) {
      // Player successfully cashed out before crash
      totalPayout += player.payout;
      winners.push({
        ...player,
        payout: player.payout
      });
      
      console.log(`🏆 Winner: ${player.username} cashed out at ${player.cashOutAt.toFixed(2)}x for ${player.payout.toFixed(2)} USDC`);
    } else if (!player.isCashedOut) {
      // Player didn't cash out, lost their bet
      losers.push(player);
      
      dbOperations.push(
        safeUpdateUserStats(player.userId, player.amount, 0),
        safeCrashDatabase('updateBetResult', player.betId, false, 0)
      );
      
      console.log(`😔 Loser: ${player.username} lost ${player.amount} USDC (didn't cash out)`);
    } else {
      // Player tried to cash out after crash (shouldn't happen, but safety check)
      losers.push(player);
      console.log(`😔 Loser: ${player.username} lost ${player.amount} USDC (cashed out too late)`);
    }
  }

  console.log(`🚀 Executing ${dbOperations.length} database operations in parallel...`);
  const startTime = Date.now();
  
  try {
    await Promise.all(dbOperations);
    console.log(`✅ All crash database operations completed in ${Date.now() - startTime}ms`);
  } catch (error) {
    console.error('❌ Error in crash database operations:', error);
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

  try {
    await safeCrashDatabase('completeGame', gameId, {
      crashPoint: finalCrashPoint,
      totalWagered,
      totalPayout,
      playersCount: gameState.crash.players.size
    });
  } catch (error) {
    console.error(`❌ Error completing crash game in database:`, error);
  }

  gameState.crash.history.unshift(gameResult);
  if (gameState.crash.history.length > 20) {
    gameState.crash.history = gameState.crash.history.slice(0, 20);
  }

  console.log(`🚀 Game ${gameId} completed - ${winners.length} winners, ${losers.length} losers, ${totalWagered} wagered, ${totalPayout} paid out`);

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
    console.error(`❌ Error broadcasting crash game results:`, error);
  }

  console.log(`🚀 Game ${gameId} - all results broadcast`);

  gameState.crash.isProcessing = false;
  
  setTimeout(() => {
    console.log(`🚀 Starting next crash game immediately after completion of ${gameId}`);
    startNewCrashGame();
  }, 3000);
}

// Start the server
httpServer.listen(port, '0.0.0.0', () => {
  console.log(`🚀 Socket.IO server running on port ${port}`);
  console.log(`🌐 CORS enabled for Vercel domains`);
  console.log(`💾 Database connection: ${UserDatabase && DiceDatabase && CrashDatabase ? 'Connected' : 'Mock mode'}`);
  
  // Start both game loops
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
