var async = require('async');
var ip = require("ip");
var bignum = require('bignumber');
var Router = require('../utils/router.js');
var sandboxHelper = require('../utils/sandbox.js');
var slots = require('../utils/slots.js');

require('colors');

// Private fields
var modules, library, self, private = {}, shared = {};

private.loaded = false;
private.isActive = false;
private.loadingLastBlock = null;
private.genesisBlock = null;
private.total = 0;
private.blocksToSync = 0;
private.syncIntervalId = null;

// Constructor
function Loader(cb, scope) {
  library = scope;
  private.genesisBlock = private.loadingLastBlock = library.genesisblock;
  self = this;
  self.__private = private;
  private.attachApi();

  setImmediate(cb, null, self);
}

// Private methods
private.attachApi = function () {
  var router = new Router();

  router.map(shared, {
    "get /status": "status",
    "get /status/sync": "sync"
  });

  library.network.app.use('/api/loader', router);
  library.network.app.use(function (err, req, res, next) {
    if (!err) return next();
    library.logger.error(req.url, err.toString());
    res.status(500).send({ success: false, error: err.toString() });
  });
}

private.syncTrigger = function (turnOn) {
  if (turnOn === false && private.syncIntervalId) {
    clearTimeout(private.syncIntervalId);
    private.syncIntervalId = null;
  }
  if (turnOn === true && !private.syncIntervalId) {
    setImmediate(function nextSyncTrigger() {
      library.network.io.sockets.emit('loader/sync', {
        blocks: private.blocksToSync,
        height: modules.blocks.getLastBlock().height
      });
      private.syncIntervalId = setTimeout(nextSyncTrigger, 1000);
    });
  }
}

private.loadFullDb = function (peer, cb) {  // 创世块的话，直接从peer加载全部数据
  var peerStr = peer ? ip.fromLong(peer.ip) + ":" + peer.port : 'unknown';

  var commonBlockId = private.genesisBlock.block.id;  // 因为是创世块，所以commomblock只能是这个

  library.logger.debug("Loading blocks from genesis from " + peerStr);

  modules.blocks.loadBlocksFromPeer(peer, commonBlockId, cb);
}

private.findUpdate = function (lastBlock, peer, cb) { // 查找commonblock然后执行undo或者直接从peer加载新区块
  var peerStr = peer ? ip.fromLong(peer.ip) + ":" + peer.port : 'unknown';  // str的peer

  library.logger.info("Looking for common block with " + peerStr);  // 查找共同区块

  modules.blocks.getCommonBlock(peer, lastBlock.height, function (err, commonBlock) {   // 根据本地区块最新高度调用getCommonBlock函数查找共同区块
    if (err || !commonBlock) {  // 如果获取时有err或者commonblock为空
      library.logger.error("Failed to get common block", err);  // 记录日志
      return cb();  // 直接执行findUpdate的回调函数，不执行下面那些逻辑
    }

    console.log('commonBlock:', commonBlock)
    library.logger.info("Found common block " + commonBlock.id + " (at " + commonBlock.height + ")" + " with peer " + peerStr + ", last block height is " + lastBlock.height);  // 记录日志
    var toRemove = lastBlock.height - commonBlock.height; // 计算本地高度和commomblock高度差

    if (toRemove >= 5) {  // 本地高度和commomblock高度差大于5
      library.logger.error("long fork, ban 60 min", peerStr); // 如果高度差超过5，说明peer高度太低，记录日志
      modules.peer.state(peer.ip, peer.port, 0, 3600);  // 本地db更新peer状态为0，该peer不可用的意思
      return cb();  // 直接执行findUpdate的回调函数，不再进行下面那些逻辑
    }

    var unconfirmedTrs = modules.transactions.getUnconfirmedTransactionList(true);  // 获得未确认交易列表
    modules.transactions.undoUnconfirmedList(function (err) { // undoUnconfirmedList的回调函数
      if (err) {
        library.logger.error('Failed to undo uncomfirmed transactions', err);
        return process.exit(0);
      }

      function rollbackBlocks(cb) { // 定义回滚块的函数
        if (commonBlock.id == lastBlock.id) { // 如果本地最新的块id等于commomblockid，那么直接回调，其实就是不需要回滚
          return cb();
        }

        async.series([
          function (next) {
            var currentRound = modules.round.calc(lastBlock.height);  // 本地当前round
            var backRound = modules.round.calc(commonBlock.height); // 要回滚到的commomblock所在round
            var backHeight = commonBlock.height;  // 回滚高度就是commomblock高度
            if (currentRound != backRound || lastBlock.height % 101 === 0) {  // 跨轮回滚或者当前块是本轮最后一个块
              if (backRound == 1) { // 如果是第1轮
                backHeight = 1; // 那么直接回滚到高度1，因为这轮只有一个1个高度
              } else {  // 要回滚到的commomblock所在轮不是1
                backHeight = backHeight - backHeight % 101; // 回滚高度=上轮最后一个块高度 
              }
              
              modules.blocks.getBlock({ height: backHeight }, function (err, result) {  // 获取回滚区块高度详情
                if (result && result.block) {
                  commonBlock = result.block;
                }
                next(err);
              })
            } else {  // 其它情况就是同轮内的回滚，回滚到commonblock，并且小于5个块
              next();
            }
          },
          function (next) {
            library.logger.info('start to roll back blocks before ' + commonBlock.height);  // 记录回滚日志
            modules.round.directionSwap('backward', lastBlock, next); // 调用directionSwap去做回滚，删除mem_round表这一轮的信息
          },
          function (next) {
            library.bus.message('deleteBlocksBefore', commonBlock); // 发送事件？
            modules.blocks.deleteBlocksBefore(commonBlock, next); // 删除区块？
          },
          function (next) {
            modules.round.directionSwap('forward', lastBlock, next);
          }
        ], function (err) {
          if (err) {
            library.logger.error("Failed to rollback blocks before " + commonBlock.height, err);
            process.exit(1);
            return;
          }
          cb(); // 回滚没有问题，cb
        });
      }

      async.series([
        async.apply(rollbackBlocks),  // apply回滚块函数，如果不需要回滚那么直接运行下面的函数
        function (next) { // 开始从peer加载区块
          library.logger.debug("Loading blocks from peer " + peerStr);  // 记录日志

          modules.blocks.loadBlocksFromPeer(peer, commonBlock.id, function (err, lastValidBlock) {
            if (err) {  // loadBlocksFromPeer的回调函数
              library.logger.error("Failed to load blocks, ban 60 min: " + peerStr, err);
              modules.peer.state(peer.ip, peer.port, 0, 3600);
            }
            next();
          });
        },
        function (next) { // 处理未确认交易
          modules.transactions.receiveTransactions(unconfirmedTrs, function (err) { // 验证后进行广播？
            if (err) {
              library.logger.error('Failed to redo unconfirmed transactions', err);
            }
            next();
          });
        }
      ], cb)
    });
  });
}

private.loadBlocks = function (lastBlock, cb) {
  modules.transport.getFromRandomPeer({
    api: '/height',
    method: 'GET'
  }, function (err, data) { // getFromRandomPeer的回调函数
    var peerStr = data && data.peer ? ip.fromLong(data.peer.ip) + ":" + data.peer.port : 'unknown';
    if (err || !data.body) {
      library.logger.log("Failed to get height from peer: " + peerStr);
      return cb();
    }

    library.logger.info("Check blockchain on " + peerStr);

    data.body.height = parseInt(data.body.height);  // peer的高度

    var report = library.scheme.validate(data.body, { // 
      type: "object",
      properties: {
        "height": {
          type: "integer",
          minimum: 0
        }
      }, required: ['height']
    });

    if (!report) {
      library.logger.log("Failed to parse blockchain height: " + peerStr + "\n" + library.scheme.getLastError());
      return cb();
    }

    if (bignum(modules.blocks.getLastBlock().height).lt(data.body.height)) { // Diff in chainbases 本地高度比peer小？
      private.blocksToSync = data.body.height;  // 将本地的待同步高度设置为peer的高度

      if (lastBlock.id != private.genesisBlock.block.id) { // Have to find common block 如果不是创世块开始同步，则需要执行findUpdate
        private.findUpdate(lastBlock, data.peer, cb);
      } else { // Have to load full db  // 创世块直接loadFullDb
        private.loadFullDb(data.peer, cb);
      }
    } else {
      cb();
    }
  });
}

private.loadSignatures = function (cb) {
  modules.transport.getFromRandomPeer({
    api: '/signatures',
    method: 'GET',
    not_ban: true
  }, function (err, data) {
    if (err) {
      return cb();
    }

    library.scheme.validate(data.body, {
      type: "object",
      properties: {
        signatures: {
          type: "array",
          uniqueItems: true
        }
      },
      required: ['signatures']
    }, function (err) {
      if (err) {
        return cb();
      }

      library.sequence.add(function loadSignatures(cb) {
        async.eachSeries(data.body.signatures, function (signature, cb) {
          async.eachSeries(signature.signatures, function (s, cb) {
            modules.multisignatures.processSignature({
              signature: s,
              transaction: signature.transaction
            }, function (err) {
              setImmediate(cb);
            });
          }, cb);
        }, cb);
      }, cb);
    });
  });
}

private.loadUnconfirmedTransactions = function (cb) {
  modules.transport.getFromRandomPeer({
    api: '/transactions',
    method: 'GET'
  }, function (err, data) {
    if (err) {
      return cb()
    }

    var report = library.scheme.validate(data.body, {
      type: "object",
      properties: {
        transactions: {
          type: "array",
          uniqueItems: true
        }
      },
      required: ['transactions']
    });

    if (!report) {
      return cb();
    }

    var transactions = data.body.transactions;

    for (var i = 0; i < transactions.length; i++) {
      try {
        transactions[i] = library.base.transaction.objectNormalize(transactions[i]);
      } catch (e) {
        var peerStr = data.peer ? ip.fromLong(data.peer.ip) + ":" + data.peer.port : 'unknown';
        library.logger.log('Transaction ' + (transactions[i] ? transactions[i].id : 'null') + ' is not valid, ban 60 min', peerStr);
        modules.peer.state(data.peer.ip, data.peer.port, 0, 3600);
        return setImmediate(cb);
      }
    }

    var trs = [];
    for (var i = 0; i < transactions.length; ++i) {
      if (!modules.transactions.hasUnconfirmedTransaction(transactions[i])) {
        trs.push(transactions[i]);
      }
    }
    library.balancesSequence.add(function (cb) {
      modules.transactions.receiveTransactions(trs, cb);
    }, cb);
  });
}

private.loadBalances = function (cb) {
  library.model.getAllNativeBalances(function (err, results) {
    if (err) return cb('Failed to load native balances: ' + err)
    for (let i = 0; i < results.length; ++i) {
      let {address, balance} = results[i]
      library.balanceCache.setNativeBalance(address, balance)
    }
    library.balanceCache.commit()
    cb(null)
  })
}

private.loadBlockChain = function (cb) {
  var offset = 0, limit = Number(library.config.loading.loadPerIteration) || 1000;
  var verify = library.config.loading.verifyOnLoading;

  function load(count) {
    verify = true;
    private.total = count;

    library.base.account.removeTables(function (err) {
      if (err) {
        throw err;
      } else {
        library.base.account.createTables(function (err) {
          if (err) {
            throw err;
          } else {
            async.until(
              function () {
                return count < offset
              }, function (cb) {
                if (count > 1) {
                  library.logger.info("Rebuilding blockchain, current block height:" + offset);
                }
                setImmediate(function () {
                  modules.blocks.loadBlocksOffset(limit, offset, verify, function (err, lastBlockOffset) {
                    if (err) {
                      return cb(err);
                    }

                    offset = offset + limit;
                    private.loadingLastBlock = lastBlockOffset;

                    cb();
                  });
                })
              }, function (err) {
                if (err) {
                  library.logger.error('loadBlocksOffset', err);
                  if (err.block) {
                    library.logger.error('Blockchain failed at ', err.block.height)
                    modules.blocks.simpleDeleteAfterBlock(err.block.id, function (err, res) {
                      if (err) return cb(err)
                      library.logger.error('Blockchain clipped');
                      private.loadBalances(cb);
                    })
                  } else {
                    cb(err);
                  }
                } else {
                  library.logger.info('Blockchain ready');
                  private.loadBalances(cb);
                }
              }
            )
          }
        });
      }
    });
  }

  library.base.account.createTables(function (err) {
    if (err) {
      throw err;
    } else {
      library.dbLite.query("select count(*) from mem_accounts where blockId = (select id from blocks where numberOfTransactions > 0 order by height desc limit 1)", { 'count': Number }, function (err, rows) {
        if (err) {
          throw err;
        }

        var reject = !(rows[0].count);

        modules.blocks.count(function (err, count) {
          if (err) {
            return library.logger.error('Failed to count blocks', err)
          }

          library.logger.info('Blocks ' + count);

          // Check if previous loading missed
          // if (reject || verify || count == 1) {
          if (verify || count == 1) {
            load(count);
          } else {
            library.dbLite.query(
              "UPDATE mem_accounts SET u_isDelegate=isDelegate,u_secondSignature=secondSignature,u_username=username,u_balance=balance,u_delegates=delegates,u_multisignatures=multisignatures"
              , function (err, updated) {
                if (err) {
                  library.logger.error(err);
                  library.logger.info("Failed to verify db integrity 1");
                  load(count);
                } else {
                  library.dbLite.query("select a.blockId, b.id from mem_accounts a left outer join blocks b on b.id = a.blockId where b.id is null", {}, ['a_blockId', 'b_id'], function (err, rows) {
                    if (err || rows.length > 0) {
                      library.logger.error(err || "Encountered missing block, looks like node went down during block processing");
                      library.logger.info("Failed to verify db integrity 2");
                      load(count);
                    } else {
                      // Load delegates
                      library.dbLite.query("SELECT lower(hex(publicKey)) FROM mem_accounts WHERE isDelegate=1", ['publicKey'], function (err, delegates) {
                        if (err || delegates.length == 0) {
                          library.logger.error(err || "No delegates, reload database");
                          library.logger.info("Failed to verify db integrity 3");
                          load(count);
                        } else {
                          modules.blocks.loadBlocksOffset(1, count, verify, function (err, lastBlock) {
                            if (err) {
                              library.logger.error(err || "Unable to load last block");
                              library.logger.info("Failed to verify db integrity 4");
                              load(count);
                            } else {
                              library.logger.info('Blockchain ready');
                              private.loadBalances(cb);
                            }
                          });
                        }
                      });
                    }
                  });
                }
              });
          }

        });
      });
    }
  });

}

// Public methods
Loader.prototype.syncing = function () {  // 
  return !!private.syncIntervalId;
}

Loader.prototype.sandboxApi = function (call, args, cb) {
  sandboxHelper.callMethod(shared, call, args, cb);
}

Loader.prototype.startSyncBlocks = function () {  // 开启同步
  library.logger.debug('startSyncBlocks enter');  // 记录开启同步日志
  if (private.isActive || !private.loaded || self.syncing()) return;
  private.isActive = true;
  library.sequence.add(function syncBlocks(cb) {
    library.logger.debug('startSyncBlocks enter sequence');
    private.syncTrigger(true);
    var lastBlock = modules.blocks.getLastBlock();
    private.loadBlocks(lastBlock, cb);
  }, function (err) {
    err && library.logger.error('loadBlocks timer:', err);
    private.syncTrigger(false);
    private.blocksToSync = 0;

    private.isActive = false;
    library.logger.debug('startSyncBlocks end');
  });
}

// Events
Loader.prototype.onPeerReady = function () {  // 接收PeerReady事件
  setImmediate(function nextSync() {  // 异步递归调用
    var lastBlock = modules.blocks.getLastBlock();  // 本地最新的区块
    var lastSlot = slots.getSlotNumber(lastBlock.timestamp);  // 获取最新区块对应的slot
    if (slots.getNextSlot() - lastSlot >= 3) {  // 如果当前时间对应的slot > 本地最新区块slot 3个高度，就开始执行startSyncBlocks
      self.startSyncBlocks();
    }
    setTimeout(nextSync, 10 * 1000);
  });

  setImmediate(function nextLoadUnconfirmedTransactions() {
    if (!private.loaded || self.syncing()) return;
    private.loadUnconfirmedTransactions(function (err) {
      err && library.logger.error('loadUnconfirmedTransactions timer:', err);
      setTimeout(nextLoadUnconfirmedTransactions, 14 * 1000)
    });

  });

  setImmediate(function nextLoadSignatures() {
    if (!private.loaded) return;
    private.loadSignatures(function (err) {
      err && library.logger.error('loadSignatures timer:', err);

      setTimeout(nextLoadSignatures, 14 * 1000)
    });
  });
}

Loader.prototype.onBind = function (scope) {
  modules = scope;

  private.loadBlockChain(function (err) {
    if (err) {
      library.logger.error('Failed to load blockchain', err)
      return process.exit(1)
    }
    library.bus.message('blockchainReady');
  });
}

Loader.prototype.onBlockchainReady = function () {
  private.loaded = true;
}

Loader.prototype.cleanup = function (cb) {
  private.loaded = false;
  cb();
  // if (!private.isActive) {
  //   cb();
  // } else {
  //   setImmediate(function nextWatch() {
  //     if (private.isActive) {
  //       setTimeout(nextWatch, 1 * 1000)
  //     } else {
  //       cb();
  //     }
  //   });
  // }
}

// Shared
shared.status = function (req, cb) {
  cb(null, {
    loaded: private.loaded,
    now: private.loadingLastBlock.height,
    blocksCount: private.total
  });
}

shared.sync = function (req, cb) {
  cb(null, {
    syncing: self.syncing(),
    blocks: private.blocksToSync,
    height: modules.blocks.getLastBlock().height
  });
}

// Export
module.exports = Loader;
