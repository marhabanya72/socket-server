// Complete Socket.IO server for Railway deployment
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
let UserDatabase, RPSDatabase;

try {
  const mysql = require('mysql2/promise');
  
  const pool = mysql.createPool({
    host: process.env.DB_HOST?.split(':')[0] || 'db-fde-02.sparkedhost.us',
    port: parseInt(process.env.DB_HOST?.split(':')[1] || '3306'),
    user: process.env.DB_USER || 'u175260_2aWtznM6FW',
    password: process.env.DB_PASSWORD || 'giqaKuZnR72ZdQL=m.DVdtUB',
    database: process.env.DB_NAME || 's175260_casino-n1verse',
    waitForConnections: true,
  });

  // User Database functions
  UserDatabase = {
    updateUserBalance: async (userId, amount, operation) => {
      try {
        const connection = await pool.getConnection();
        const operator = operation === 'add' ? '+' : '-';
        await connection.execute(
          `UPDATE users SET balance = balance ${operator} ?, last_active = CURRENT_TIMESTAMP WHERE id = ?`,
          [Math.abs(amount), userId]
        );
        connection.release();
        console.log(`💰 Database: ${operation} ${amount} for user ${userId}`);
        return true;
      } catch (error) {
        console.error('❌ Database updateUserBalance error:', error);
        return false;
      }
    },
    updateUserStats: async (userId, wagered, won) => {
      try {
        const connection = await pool.getConnection();
        await connection.execute(
          `UPDATE users SET 
           total_wagered = total_wagered + ?,
           total_won = total_won + ?,
           games_played = games_played + 1,
           last_active = CURRENT_TIMESTAMP
           WHERE id = ?`,
          [wagered, won, userId]
        );
        connection.release();
        console.log(`📊 Database: stats updated for user ${userId} - wagered: ${wagered}, won: ${won}`);
        return true;
      } catch (error) {
        console.error('❌ Database updateUserStats error:', error);
        return false;
      }
    }
  };

  // RPS Database functions
  RPSDatabase = {
    createLobby: async (lobbyData) => {
      try {
        const connection = await pool.getConnection();
        await connection.execute(
          `INSERT INTO rps_lobbies (id, creator_id, amount, hashed_seed, timeout_at)
           VALUES (?, ?, ?, ?, DATE_ADD(NOW(), INTERVAL 30 SECOND))`,
          [lobbyData.id, lobbyData.creatorId, lobbyData.amount, lobbyData.hashedSeed]
        );
        connection.release();
        console.log(`🏆 Database: Lobby created ${lobbyData.id}`);
        return true;
      } catch (error) {
        console.error('❌ Database createLobby error:', error);
        return false;
      }
    },
    updateLobbyStatus: async (lobbyId, status, opponentId) => {
      try {
        const connection = await pool.getConnection();
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
        connection.release();
        console.log(`🔄 Database: Lobby ${lobbyId} status updated to ${status}`);
        return true;
      } catch (error) {
        console.error('❌ Database updateLobbyStatus error:', error);
        return false;
      }
    },
    createBattle: async (battleData) => {
      try {
        const connection = await pool.getConnection();
        await connection.execute(
          `INSERT INTO rps_battles (id, lobby_id, player1_id, player2_id, amount, server_seed, hashed_seed, nonce, is_vs_bot)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          [battleData.id, battleData.lobbyId, battleData.player1Id, battleData.player2Id, battleData.amount, 
           battleData.serverSeed, battleData.hashedSeed, battleData.nonce, battleData.isVsBot]
        );
        connection.release();
        console.log(`⚔️ Database: Battle created ${battleData.id}`);
        return true;
      } catch (error) {
        console.error('❌ Database createBattle error:', error);
        return false;
      }
    },
    completeBattle: async (battleId, result) => {
      try {
        const connection = await pool.getConnection();
        await connection.execute(
          `UPDATE rps_battles SET 
           player1_move = ?, player2_move = ?, winner_id = ?, payout = ?
           WHERE id = ?`,
          [result.player1Move, result.player2Move, result.winnerId, result.payout, battleId]
        );
        connection.release();
        console.log(`🏁 Database: Battle completed ${battleId}`);
        return true;
      } catch (error) {
        console.error('❌ Database completeBattle error:', error);
        return false;
      }
    },
    addUserHistory: async (historyData) => {
      try {
        const connection = await pool.getConnection();
        await connection.execute(
          `INSERT INTO rps_user_history (id, user_id, opponent_id, opponent_username, user_move, opponent_move, result, amount, payout, is_vs_bot)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          [historyData.id, historyData.userId, historyData.opponentId, historyData.opponentUsername, 
           historyData.userMove, historyData.opponentMove, historyData.result, historyData.amount, historyData.payout, historyData.isVsBot]
        );
        connection.release();
        console.log(`📜 Database: User history added for ${historyData.userId}`);
        return true;
      } catch (error) {
        console.error('❌ Database addUserHistory error:', error);
        return false;
      }
    },
    addRecentBattle: async (battleData) => {
      try {
        const connection = await pool.getConnection();
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
        connection.release();
        console.log(`🌐 Database: Recent battle added ${battleData.id}`);
        return true;
      } catch (error) {
        console.error('❌ Database addRecentBattle error:', error);
        return false;
      }
    },
    getUserHistory: async (userId, limit = 20) => {
      try {
        const connection = await pool.getConnection();
        const [rows] = await connection.execute(
          `SELECT * FROM rps_user_history 
           WHERE user_id = ? 
           ORDER BY created_at DESC 
           LIMIT ?`,
          [userId, limit]
        );
        connection.release();
        return rows;
      } catch (error) {
        console.error('❌ Database getUserHistory error:', error);
        return [];
      }
    },
    getRecentBattles: async (limit = 10) => {
      try {
        const connection = await pool.getConnection();
        const [rows] = await connection.execute(
          `SELECT * FROM rps_recent_battles 
           ORDER BY created_at DESC 
           LIMIT ?`,
          [limit]
        );
        connection.release();
        return rows;
      } catch (error) {
        console.error('❌ Database getRecentBattles error:', error);
        return [];
      }
    },
    getBattleHistory: async (limit = 10) => {
      try {
        const connection = await pool.getConnection();
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
        connection.release();
        return rows;
      } catch (error) {
        console.error('❌ Database getBattleHistory error:', error);
        return [];
      }
    }
  };
  
  console.log('✅ Database connection established');
} catch (error) {
  console.log('⚠️ Database import failed, using mock functions');
  UserDatabase = {
    updateUserBalance: async () => {
      console.log('📝 Mock: updateUserBalance called');
      return Promise.resolve(false);
    },
    updateUserStats: async () => {
      console.log('📝 Mock: updateUserStats called'); 
      return Promise.resolve(false);
    }
  };
  RPSDatabase = {
    createLobby: async () => console.log('📝 Mock: createLobby called'),
    createBattle: async () => console.log('📝 Mock: createBattle called'),
    completeBattle: async () => console.log('📝 Mock: completeBattle called'),
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

// Game state management
const gameState = {
  dice: {
    currentGame: null,
    history: [],
    players: new Map()
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
  return `dice_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
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

// Socket.IO event handlers
io.on('connection', (socket) => {
  console.log('User connected:', socket.id);

  // Store user connection
  socket.on('user-connect', (userData) => {
    gameState.connectedUsers.set(socket.id, userData);
    socket.userData = userData;
  });

  // Dice Game Events
  socket.on('join-dice', (userData) => {
    socket.join('dice-room');
    socket.userData = userData;
    
    // Send current game state
    if (gameState.dice.currentGame) {
      socket.emit('dice-game-state', gameState.dice.currentGame);
    }
  });

  socket.on('place-dice-bet', (betData) => {
    if (!gameState.dice.currentGame || gameState.dice.currentGame.phase !== 'betting') {
      socket.emit('bet-error', 'Betting is closed');
      return;
    }

    gameState.dice.players.set(socket.id, {
      userId: betData.userId,
      username: betData.username,
      amount: betData.amount,
      choice: betData.choice, // 'odd' or 'even'
      socketId: socket.id
    });

    io.to('dice-room').emit('player-joined', {
      playerId: socket.id,
      username: betData.username,
      amount: betData.amount,
      choice: betData.choice
    });
  });

  // RPS Game Events
  socket.on('join-rps', (userData) => {
    console.log('User joined RPS room:', userData.username, 'Socket ID:', socket.id);
    socket.join('rps-room');
    socket.userData = userData;
    
    // Send current lobbies to the user
    const currentLobbies = Array.from(gameState.rps.lobbies.values())
      .filter(lobby => lobby.status === 'waiting')
      .slice(0, 20);
      
    socket.emit('rps-lobbies-list', currentLobbies);
    
    // Send battle history
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

    // Save lobby to database
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

    // Broadcast to all users in RPS room
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
    
    // Notify lobby participants
    io.to(`rps-lobby-${joinData.lobbyId}`).emit('lobby-ready', lobby);
    
    // Update all users about lobby status change
    io.to('rps-room').emit('lobby-updated', lobby);
    
    console.log('Lobby joined successfully:', joinData.lobbyId);
  });

  socket.on('play-rps-bot', (botData) => {
    console.log('User requesting bot battle:', botData);
    console.log('Socket lobbyId:', socket.lobbyId);
    console.log('Available lobbies:', Array.from(gameState.rps.lobbies.keys()));
    
    // Try to find lobby by socket ID first
    let lobbyId = socket.lobbyId;
    let lobby = gameState.rps.lobbies.get(lobbyId);
    
    // If not found, try to find lobby where this user is the creator
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

    // Add bot as opponent
    lobby.opponent = {
      socketId: 'bot',
      userId: 'bot',
      username: 'Bot',
      amount: botData.amount || lobby.creator.amount,
      profilePicture: '/bot-avatar.png'
    };
    lobby.status = 'vs-bot';

    // Notify the lobby creator
    io.to(`rps-lobby-${lobbyId}`).emit('bot-joined', lobby);
    
    // Update all users about lobby status change
    io.to('rps-room').emit('lobby-updated', lobby);
    
    console.log('✅ Bot joined lobby:', lobbyId);
  });

  socket.on('submit-rps-move', async (moveData) => {
    console.log('Move submitted:', moveData);
    console.log('Socket lobbyId:', socket.lobbyId);
    
    // Try to find lobby
    let lobby = gameState.rps.lobbies.get(moveData.lobbyId);
    
    // If not found by provided ID, try to find by socket
    if (!lobby && socket.lobbyId) {
      lobby = gameState.rps.lobbies.get(socket.lobbyId);
      moveData.lobbyId = socket.lobbyId;
    }
    
    // Still not found? Search by user
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

    // Handle bot game
    if (lobby.status === 'vs-bot') {
      const botMove = generateProvablyFairRPSMove(lobby.hashedSeed, moveData.nonce);
      const result = determineRPSWinner(moveData.move, botMove);
      
      let winnerId = null;
      let payout = 0;
      const betAmount = lobby.creator.amount;
      const totalPot = betAmount * 2;
      
      // Calculate payout and update user balance
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

      // Add to history
      gameState.rps.history.unshift(battleResult);
      if (gameState.rps.history.length > 50) {
        gameState.rps.history = gameState.rps.history.slice(0, 50);
      }

      // Send result to lobby participants
      io.to(`rps-lobby-${moveData.lobbyId}`).emit('battle-result', battleResult);
      
      // Update all users with new battle history
      io.to('rps-room').emit('battle-history-updated', gameState.rps.history.slice(0, 10));
      
      // Clean up lobby
      gameState.rps.lobbies.delete(moveData.lobbyId);
      io.to('rps-room').emit('lobby-removed', moveData.lobbyId);
      
      console.log('Bot battle completed:', battleResult.id, 'Winner:', winnerId);
    }
    // Handle PvP game (player vs player)
    else if (lobby.status === 'ready') {
      // Store the move and wait for both players
      if (!gameState.rps.activeBattles.has(moveData.lobbyId)) {
        gameState.rps.activeBattles.set(moveData.lobbyId, {
          lobby: lobby,
          moves: {},
          players: [lobby.creator.userId, lobby.opponent.userId],
          submittedCount: 0
        });
      }

      const battle = gameState.rps.activeBattles.get(moveData.lobbyId);
      
      // Only store move if not already submitted by this user
      if (!battle.moves[socket.userData.userId]) {
        battle.moves[socket.userData.userId] = moveData.move;
        battle.submittedCount++;
        
        console.log(`Move submitted by ${socket.userData.username}: ${moveData.move} (${battle.submittedCount}/2)`);
        
        // Notify the player that their move was submitted
        socket.emit('move-submitted', { 
          message: 'Move submitted! Waiting for opponent...',
          movesSubmitted: battle.submittedCount,
          totalPlayers: 2
        });
        
        // Notify both players about move count (without revealing moves)
        io.to(`rps-lobby-${moveData.lobbyId}`).emit('moves-update', {
          movesSubmitted: battle.submittedCount,
          totalPlayers: 2,
          waiting: battle.submittedCount < 2
        });
      } else {
        // User already submitted a move
        socket.emit('move-error', 'You have already submitted your move');
        return;
      }

      // If both moves are submitted, determine winner
      if (battle.submittedCount === 2 && Object.keys(battle.moves).length === 2) {
        console.log('Both moves submitted, determining winner...');
        
        const move1 = battle.moves[lobby.creator.userId];
        const move2 = battle.moves[lobby.opponent.userId];
        
        console.log(`PvP Battle: ${lobby.creator.username} (${move1}) vs ${lobby.opponent.username} (${move2})`);
        
        const result = determineRPSWinner(move1, move2);
        
        let winnerId = null;
        let payout = 0;
        const totalPot = lobby.creator.amount + lobby.opponent.amount;
        
        // Calculate payout and update user balances
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

        // Update lobby status to completed
        await safeRPSDatabase('updateLobbyStatus', moveData.lobbyId, 'completed');

        // Save battle to database
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

        // Complete the battle with moves and results
        await safeRPSDatabase('completeBattle', battleResult.id, {
          player1Move: move1,
          player2Move: move2,
          winnerId: winnerId === 'draw' ? null : winnerId,
          payout: payout
        });

        // Add to both players' personal history
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

        // Player 1 history
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

        // Player 2 history
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

        // Add to recent battles (public)
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

        // Add to memory history for immediate updates
        gameState.rps.history.unshift(battleResult);
        if (gameState.rps.history.length > 50) {
          gameState.rps.history = gameState.rps.history.slice(0, 50);
        }

        // Send result to both players in the lobby
        io.to(`rps-lobby-${moveData.lobbyId}`).emit('battle-result', battleResult);
        
        // Send fresh battle history from database to all users
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
          // Fallback to memory history
          io.to('rps-room').emit('battle-history-updated', gameState.rps.history.slice(0, 10));
        }
        
        // Clean up
        gameState.rps.activeBattles.delete(moveData.lobbyId);
        gameState.rps.lobbies.delete(moveData.lobbyId);
        io.to('rps-room').emit('lobby-removed', moveData.lobbyId);
        
        console.log('✅ PvP battle completed:', battleResult.id, 'Winner:', winnerId, 'Payout:', payout);
      } else {
        console.log(`Waiting for more moves: ${battle.submittedCount}/2 submitted`);
      }
    }
  });

  // Chat Events
  socket.on('join-chat', async (userData) => {
    socket.join('chat-room');
    
    // Look up user's profile picture from database
    const userProfilePicture = await getUserProfilePicture(userData.userId);
    const defaultAvatar = `https://api.dicebear.com/7.x/bottts-neutral/svg?seed=${userData.userId || userData.username}&backgroundColor=1a202c&primaryColor=fa8072`;
    
    // Store complete user data on socket
    socket.userData = {
      ...userData,
      profilePicture: userProfilePicture || userData.profilePicture || userData.profile_picture || defaultAvatar
    };
    
    console.log(`👤 User joined chat: ${userData.username} with profile: ${socket.userData.profilePicture}`);
    
    // Send recent chat history with corrected profile pictures
    const correctedHistory = await Promise.all(
      gameState.chat.messages.slice(-50).map(async (msg) => {
        if (!msg.profilePicture || msg.profilePicture === '/default-avatar.png') {
          const correctProfilePicture = await getUserProfilePicture(msg.userId);
          return {
            ...msg,
            profilePicture: correctProfilePicture || `https://api.dicebear.com/7.x/bottts-neutral/svg?seed=${msg.userId || msg.username}&backgroundColor=1a202c&primaryColor=fa8072`
          };
        }
        return msg;
      })
    );
    
    socket.emit('chat-history', correctedHistory);
    
    // Update online users count
    const onlineCount = io.sockets.adapter.rooms.get('chat-room')?.size || 0;
    io.to('chat-room').emit('online-users-count', onlineCount);
  });

  socket.on('send-message', async (messageData) => {
    try {
      // Look up user's profile picture from database
      const userProfilePicture = await getUserProfilePicture(messageData.userId);
      
      // Generate default avatar if no profile picture found
      const defaultAvatar = `https://api.dicebear.com/7.x/bottts-neutral/svg?seed=${messageData.userId || messageData.username}&backgroundColor=1a202c&primaryColor=fa8072`;
      
      const message = {
        id: Date.now(),
        userId: messageData.userId,
        username: messageData.username,
        message: messageData.message,
        timestamp: new Date(),
        profilePicture: userProfilePicture || messageData.profilePicture || socket.userData?.profilePicture || defaultAvatar
      };

      gameState.chat.messages.push(message);
      if (gameState.chat.messages.length > 100) {
        gameState.chat.messages = gameState.chat.messages.slice(-100);
      }

      console.log(`💬 Chat message from ${messageData.username}: ${messageData.message}`);
      console.log(`🖼️ Profile picture: ${message.profilePicture}`);

      io.to('chat-room').emit('new-message', message);
    } catch (error) {
      console.error('Error sending message:', error);
      // Fallback to original logic if database lookup fails
      const defaultAvatar = `https://api.dicebear.com/7.x/bottts-neutral/svg?seed=${messageData.userId || messageData.username}&backgroundColor=1a202c&primaryColor=fa8072`;
      
      const message = {
        id: Date.now(),
        userId: messageData.userId,
        username: messageData.username,
        message: messageData.message,
        timestamp: new Date(),
        profilePicture: messageData.profilePicture || socket.userData?.profilePicture || defaultAvatar
      };

      gameState.chat.messages.push(message);
      io.to('chat-room').emit('new-message', message);
    }
  });

  // Admin Events
  socket.on('admin-join', (adminData) => {
    socket.join('admin-room');
    socket.emit('admin-dashboard-data', {
      dice: gameState.dice,
      rps: gameState.rps,
      chat: gameState.chat
    });
  });

  socket.on('disconnect', () => {
    console.log('User disconnected:', socket.id);
    
    // Clean up dice game
    gameState.dice.players.delete(socket.id);
    
    // Clean up RPS lobbies
    if (socket.lobbyId) {
      const lobby = gameState.rps.lobbies.get(socket.lobbyId);
      if (lobby && lobby.creator.socketId === socket.id) {
        gameState.rps.lobbies.delete(socket.lobbyId);
        io.to('rps-room').emit('lobby-removed', socket.lobbyId);
        console.log('Lobby removed due to creator disconnect:', socket.lobbyId);
      }
    }
    
    // Remove from connected users
    gameState.connectedUsers.delete(socket.id);
    
    // Update online chat users count
    const onlineCount = io.sockets.adapter.rooms.get('chat-room')?.size || 0;
    io.to('chat-room').emit('online-users-count', onlineCount);
  });
});

// Dice game loop - runs every 30 seconds
function startDiceGameLoop() {
  setInterval(() => {
    startNewDiceGame();
  }, 30000); // 30 seconds

  // Start first game immediately
  startNewDiceGame();
}

function startNewDiceGame() {
  const gameId = generateGameId();
  const serverSeed = generateServerSeed();
  const hashedSeed = generateHash(serverSeed);

  gameState.dice.currentGame = {
    id: gameId,
    serverSeed,
    hashedSeed,
    phase: 'betting',
    timeLeft: 25,
    result: null,
    players: new Map()
  };

  gameState.dice.players.clear();

  io.to('dice-room').emit('new-dice-game', {
    gameId,
    hashedSeed,
    phase: 'betting',
    timeLeft: 25
  });

  // Betting phase countdown
  const bettingInterval = setInterval(() => {
    gameState.dice.currentGame.timeLeft--;
    io.to('dice-room').emit('dice-timer-update', gameState.dice.currentGame.timeLeft);

    if (gameState.dice.currentGame.timeLeft <= 0) {
      clearInterval(bettingInterval);
      startDiceRolling();
    }
  }, 1000);
}

function startDiceRolling() {
  gameState.dice.currentGame.phase = 'rolling';
  gameState.dice.currentGame.timeLeft = 5;

  io.to('dice-room').emit('dice-rolling-start');

  // Rolling phase
  setTimeout(() => {
    const result = generateProvablyFairDiceResult(
      gameState.dice.currentGame.serverSeed,
      gameState.dice.currentGame.id
    );

    gameState.dice.currentGame.result = result;
    gameState.dice.currentGame.phase = 'complete';

    // Calculate winners and payouts
    const winners = [];
    const losers = [];

    gameState.dice.players.forEach((player, socketId) => {
      const isWinner = (result.isOdd && player.choice === 'odd') || 
                      (!result.isOdd && player.choice === 'even');
      
      if (isWinner) {
        const payout = player.amount * 1.96; // 2% house edge
        winners.push({
          ...player,
          payout
        });
      } else {
        losers.push(player);
      }
    });

    const gameResult = {
      gameId: gameState.dice.currentGame.id,
      diceValue: result.value,
      isOdd: result.isOdd,
      serverSeed: gameState.dice.currentGame.serverSeed,
      hashedSeed: gameState.dice.currentGame.hashedSeed,
      winners,
      losers,
      timestamp: new Date()
    };

    gameState.dice.history.unshift(gameResult);
    if (gameState.dice.history.length > 20) {
      gameState.dice.history = gameState.dice.history.slice(0, 20);
    }

    io.to('dice-room').emit('dice-result', gameResult);
    io.to('admin-room').emit('dice-game-complete', gameResult);

  }, 5000);
}

// Start the server
httpServer.listen(port, '0.0.0.0', () => {
  console.log(`🚀 Socket.IO server running on port ${port}`);
  console.log(`🌐 CORS enabled for Vercel domains`);
  console.log(`💾 Database connection: ${UserDatabase ? 'Connected' : 'Mock mode'}`);
  
  // Start dice game loop
  startDiceGameLoop();
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('SIGTERM received, shutting down gracefully');
  httpServer.close(() => {
    console.log('Process terminated');
  });
});

process.on('SIGINT', () => {
  console.log('SIGINT received, shutting down gracefully');
  httpServer.close(() => {
    console.log('Process terminated');
  });
});
