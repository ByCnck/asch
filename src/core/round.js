var async = require('async');
var slots = require('../utils/slots.js'); // 产快时间10s、受托人个数是101个，都在这里面定义
var sandboxHelper = require('../utils/sandbox.js');
var constants = require('../utils/constants.js'); // 本文件没有用到

// Private fields // 声明私有变量，只有本文件可以访问，因为没有通过exports暴露出去，是commjs规范
var modules, library, self, private = {}, shared = {};  

private.loaded = false;

private.feesByRound = {};
private.rewardsByRound = {};
private.delegatesByRound = {};
private.unFeesByRound = {};
private.unRewardsByRound = {};
private.unDelegatesByRound = {};

const CLUB_BONUS_RATIO = 0.2

// Constructor  // 构造函数？
function Round(cb, scope) {
  library = scope;
  self = this;
  self.__private = private;
  setImmediate(cb, null, self);
}

// Round changes
function RoundChanges(round, back) { // 对象，类
  if (!back) {
    var roundFees = parseInt(private.feesByRound[round]) || 0;  // 该轮的手续费，Int值
    var roundRewards = (private.rewardsByRound[round] || []); // 该轮的奖励，数组
  } else {
    var roundFees = parseInt(private.unFeesByRound[round]) || 0;  // 该轮的手续费是未确认的
    var roundRewards = (private.unRewardsByRound[round] || []); // 该轮的奖励是未确认的
  }

  this.at = function (index) {  // this此时指的是对象自身，at方法
    var ratio = global.featureSwitch.enableClubBonus ? (1 - CLUB_BONUS_RATIO) : 1
    var totalDistributeFees = Math.floor(roundFees * ratio) 
    var fees = Math.floor(totalDistributeFees / slots.delegates)  // 前101受托人平均分的手续费的整数值，比如1.2、1.6的结果都为1
    var feesRemaining = totalDistributeFees - (fees * slots.delegates)  // 手续费平均分配后余额
    var rewards = Math.floor(parseInt(roundRewards[index]) * ratio) || 0  //根据受托人的index去查他的奖励

    return {
      fees: fees, // 每个受托人的手续费奖励
      feesRemaining: feesRemaining, // 手续费奖励余额
      rewards: rewards, // 每个受托人的产块奖励
      balance: fees + rewards // 每个受托人应该得到的奖励总和
    };
  }

  this.getClubBonus = function () {
    var fees = roundFees - Math.floor(roundFees * (1 - CLUB_BONUS_RATIO))
    var rewards = 0
    for (let i = 0; i < roundRewards.length; ++i) {
      let reward = parseInt(roundRewards[i])
      rewards += (reward - Math.floor(reward * (1 - CLUB_BONUS_RATIO)))
    }
    return fees + rewards
  }
}

Round.prototype.loaded = function () {  // 获取private.loaded值
  return private.loaded;
}

// Public methods
Round.prototype.calc = function (height) {  // 根据高度计算轮次
  return Math.floor(height / slots.delegates) + (height % slots.delegates > 0 ? 1 : 0);
}

Round.prototype.getVotes = function (round, cb) { // 查询每轮的得票情况，这个sql可以优化
  library.dbLite.query("select delegate, amount from ( " +
    "select m.delegate, sum(m.amount) amount, m.round from mem_round m " +
    "group by m.delegate, m.round " +
    ") where round = $round", { round: round }, { delegate: String, amount: Number }, function (err, rows) {
      cb(err, rows) // 返回err和rows数组？
    });
}

Round.prototype.flush = function (round, cb) {  // 删除本轮的mem_round表数据
  library.dbLite.query("delete from mem_round where round = $round", { round: round }, cb);
}

Round.prototype.directionSwap = function (direction, lastBlock, cb) { // 本文件没有使用这个函数，在replay-block.js和loader.js中有使用
  if (direction == 'backward') {
    private.feesByRound = {};
    private.rewardsByRound = {};
    private.delegatesByRound = {};
    self.flush(self.calc(lastBlock.height), cb);
  } else {
    private.unFeesByRound = {};
    private.unRewardsByRound = {};
    private.unDelegatesByRound = {};
    self.flush(self.calc(lastBlock.height), cb);
  }
}

Round.prototype.backwardTick = function (block, previousBlock, cb) {  //这是回滚的？
  function done(err) {
    cb && cb(err);
  }

  modules.accounts.mergeAccountAndGet({
    publicKey: block.generatorPublicKey,
    producedblocks: -1,
    blockId: block.id,
    round: modules.round.calc(block.height)
  }, function (err) {
    if (err) {
      return done(err);
    }

    var round = self.calc(block.height);

    var prevRound = self.calc(previousBlock.height);

    private.unFeesByRound[round] = (private.unFeesByRound[round] || 0);
    private.unFeesByRound[round] += block.totalFee;

    private.unRewardsByRound[round] = (private.unRewardsByRound[round] || []);
    private.unRewardsByRound[round].push(block.reward);

    private.unDelegatesByRound[round] = private.unDelegatesByRound[round] || [];
    private.unDelegatesByRound[round].push(block.generatorPublicKey);

    if (prevRound === round && previousBlock.height !== 1) {
      return done();
    }

    if (private.unDelegatesByRound[round].length !== slots.delegates && previousBlock.height !== 1) {
      return done();
    }
    var outsiders = [];
    async.series([  //串行执行下面4个函数
      function (cb) { //根据区块高度生成受托人公钥列表？追加受托人地址到outsiders数组中
        if (block.height === 1) {
          return cb();
        }
        modules.delegates.generateDelegateList(block.height, function (err, roundDelegates) {
          if (err) {
            return cb(err);
          }
          for (var i = 0; i < roundDelegates.length; i++) {
            if (private.unDelegatesByRound[round].indexOf(roundDelegates[i]) == -1) {
              if (global.featureSwitch.fixVoteNewAddressIssue) {
                outsiders.push(modules.accounts.generateAddressByPublicKey2(roundDelegates[i]));
              } else {
                outsiders.push(modules.accounts.generateAddressByPublicKey(roundDelegates[i]));
              }
            }
          }
          cb();
        });
      },
      function (cb) { //更新mem_accounts表，受托人地址的missedblocks-1
        if (!outsiders.length) {
          return cb();
        }
        var escaped = outsiders.map(function (item) {
          return "'" + item + "'";
        });
        library.dbLite.query('update mem_accounts set missedblocks = missedblocks - 1 where address in (' + escaped.join(',') + ')', function (err, data) {
          cb(err);
        });
      },
      // function (cb) {
      //   self.getVotes(round, function (err, votes) {
      //     if (err) {
      //       return cb(err);
      //     }
      //     async.eachSeries(votes, function (vote, cb) {
      //       library.dbLite.query('update mem_accounts set vote = vote + $amount where address = $address', {
      //         address: modules.accounts.generateAddressByPublicKey(vote.delegate),
      //         amount: vote.amount
      //       }, cb);
      //     }, function (err) {
      //       self.flush(round, function (err2) {
      //         cb(err || err2);
      //       });
      //     })
      //   });
      // },
      function (cb) { //没看懂
        var roundChanges = new RoundChanges(round, true); //对象实例化，并且是回滚的，back===true

        async.forEachOfSeries(private.unDelegatesByRound[round], function (delegate, index, next) {
          var changes = roundChanges.at(index);
          var changeBalance = changes.balance;
          var changeFees = changes.fees;
          var changeRewards = changes.rewards;

          if (index === 0) {
            changeBalance += changes.feesRemaining;
            changeFees += changes.feesRemaining;
          }

          modules.accounts.mergeAccountAndGet({
            publicKey: delegate,
            balance: -changeBalance,
            u_balance: -changeBalance,
            blockId: block.id,
            round: modules.round.calc(block.height),
            fees: -changeFees,
            rewards: -changeRewards
          }, next);
        }, cb);
      },
      function (cb) { //更新mem_accounts表，根据受托人地址更新其得票信息
        // distribute club bonus
        if (!global.featureSwitch.enableClubBonus) {
          return cb()
        }
        var bonus = '-' + new RoundChanges(round).getClubBonus()
        var dappId = global.state.clubInfo.transactionId
        const BONUS_CURRENCY = 'XAS'
        library.logger.info('Asch witness club get new bonus: ' + bonus)
        library.balanceCache.addAssetBalance(dappId, BONUS_CURRENCY, bonus)
        library.model.updateAssetBalance(BONUS_CURRENCY, bonus, dappId, cb)
      },
      function (cb) {
        self.getVotes(round, function (err, votes) {
          if (err) {
            return cb(err);
          }
          async.eachSeries(votes, function (vote, cb) {
            var address = null
            if (global.featureSwitch.fixVoteNewAddressIssue) {
              address = modules.accounts.generateAddressByPublicKey2(vote.delegate)
            } else {
              address = modules.accounts.generateAddressByPublicKey(vote.delegate)
            }
            library.dbLite.query('update mem_accounts set vote = vote + $amount where address = $address', {
              address: address,
              amount: vote.amount
            }, cb);
          }, function (err) {
            self.flush(round, function (err2) { //删除mem_round表这一轮的信息
              cb(err || err2);
            });
          })
        });
      }
    ], function (err) {
      delete private.unFeesByRound[round];  //删除这一轮的信息，为什么删除变量
      delete private.unRewardsByRound[round];
      delete private.unDelegatesByRound[round];
      done(err)
    });
  });
}

Round.prototype.tick = function (block, cb) { // 本轮?还是本区块结束后发放奖励，在/src/core/blocks.js中调用  本轮最后一个区块发放奖励并更新投票
  function done(err) {
    if (err) {
      library.logger.error("Round tick failed: " + err);
    } else {
      library.logger.debug("Round tick completed", {
        block: block
      });
    }
    cb && setImmediate(cb, err);  // 有回调函数，那么立即加入到队列里面，实质还是异步的。注意：跟cb()还是不一样的，这是同步的
  }

  modules.accounts.mergeAccountAndGet({ // 这次调用是为了更新受托人产块数+1，mergeAccountAndGet实质调用的是library.base.account.merge(address, data, cb);
    publicKey: block.generatorPublicKey,  // 产生该区块的受托人公钥
    producedblocks: 1,  // 产块数+1
    blockId: block.id,  // 区块id
    round: modules.round.calc(block.height) // 该区块所在轮次
  }, function (err) { // mergeAccountAndGet的回调函数
    if (err) {
      return done(err);
    }
    var round = self.calc(block.height);  // 根据块高度计算轮次

    private.feesByRound[round] = (private.feesByRound[round] || 0);
    private.feesByRound[round] += block.totalFee; // 该轮所有区块的手续费总和

    private.rewardsByRound[round] = (private.rewardsByRound[round] || []);
    private.rewardsByRound[round].push(block.reward); // 该轮所有区块奖励数组

    private.delegatesByRound[round] = private.delegatesByRound[round] || [];  // 如果这个变量不存在（本轮首块），那么就定义为空数组，这个操作是每个块都要进行的，所以是累加的（101次）
    private.delegatesByRound[round].push(block.generatorPublicKey); // 该轮所有产快受托人公钥数组

    var nextRound = self.calc(block.height + 1);  // 根据下一个区块高度计算所在轮次

    if (round === nextRound && block.height !== 1) {  // 如果下一个块高度和当前块在同一轮里面且当前块高度不是1，就直接返回done函数。不是本轮的最后一块，直接返回done
      return done();
    }
    
    if (private.delegatesByRound[round].length !== slots.delegates && block.height !== 1 && block.height !== 101) { // 产快的受托人没满101 && 高度不是1 && 高度不是101，就是第2轮是100直接完成。从第三轮开始才是101
      return done();
    }
    
    var outsiders = []; // 没有正常产块的受托人地址数组

    async.series([  // 串行执行下面几个函数，这些操作都是本轮最后一个块时执行的
      function (cb) { // 根据区块高度生成受托人公钥列表,没有产快的受托人地址到outsiders数组中
        if (block.height === 1) { // 如果区块高度为1，直接返回回调，本轮直接完成，不需要101。高度1本身就是一轮。
          return cb();
        }
        modules.delegates.generateDelegateList(block.height, function (err, roundDelegates) { 
          if (err) {
            return cb(err);
          }
          for (var i = 0; i < roundDelegates.length; i++) { // 对 generateDelegateList结果进行处理
            if (private.delegatesByRound[round].indexOf(roundDelegates[i]) == -1) { // 
              if (global.featureSwitch.fixVoteNewAddressIssue) {
                outsiders.push(modules.accounts.generateAddressByPublicKey2(roundDelegates[i]));
              } else {
                outsiders.push(modules.accounts.generateAddressByPublicKey(roundDelegates[i]));
              }
            }
          }
          cb(); // 回调
        });
      },
      function (cb) { // 更新mem_accounts表，丢块受托人地址的missedblocks+1
        if (!outsiders.length) {  //如果丢块受托人地址列表为空，则直接返回回调
          return cb();
        }
        var escaped = outsiders.map(function (item) { 
          return "'" + item + "'";
        });
        library.dbLite.query('update mem_accounts set missedblocks = missedblocks + 1 where address in (' + escaped.join(',') + ')', function (err, data) { // 丢块数加1
          cb(err);
        });
      },
      // function (cb) {
      //   self.getVotes(round, function (err, votes) {
      //     if (err) {
      //       return cb(err);
      //     }
      //     async.eachSeries(votes, function (vote, cb) {
      //       library.dbLite.query('update mem_accounts set vote = vote + $amount where address = $address', {
      //         address: modules.accounts.generateAddressByPublicKey(vote.delegate),
      //         amount: vote.amount
      //       }, cb);
      //     }, function (err) {
      //       self.flush(round, function (err2) {
      //         cb(err || err2);
      //       });
      //     })
      //   });
      // },
      function (cb) { // 更新每个受托人的奖励，进行mergeAccountAndGet
        var roundChanges = new RoundChanges(round); //对象实例化，非回滚

        async.forEachOfSeries(private.delegatesByRound[round], function (delegate, index, next) { // forEachOfSeries对受托人公钥数组进行处理后，把delegate, index传给回调函数
          var changes = roundChanges.at(index); // roundChanges实例调用at方法，拿到受托人各自对应的手续费、手续费余额、产快奖励、奖励总和4个元素的对象
          var changeBalance = changes.balance;
          var changeFees = changes.fees;
          var changeRewards = changes.rewards;
          if (index === private.delegatesByRound[round].length - 1) { // 如果是这轮最后一个出块的受托人，可以拿到手续费余额feesRemaining
            changeBalance += changes.feesRemaining;
            changeFees += changes.feesRemaining;
          }

          modules.accounts.mergeAccountAndGet({ // 每个受托人余额更新
            publicKey: delegate,
            balance: changeBalance,
            u_balance: changeBalance,
            blockId: block.id,
            round: modules.round.calc(block.height),
            fees: changeFees,
            rewards: changeRewards
          }, next); //next下一个继续
        }, cb); //async.forEachOfSeries结束，回调cb
      },
      function (cb) {
        // distribute club bonus
        if (!global.featureSwitch.enableClubBonus) {
          return cb()
        }
        var bonus = new RoundChanges(round).getClubBonus()
        var dappId = global.state.clubInfo.transactionId
        const BONUS_CURRENCY = 'XAS'
        library.logger.info('Asch witness club get new bonus: ' + bonus)
        library.balanceCache.addAssetBalance(dappId, BONUS_CURRENCY, bonus)
        library.model.updateAssetBalance(BONUS_CURRENCY, bonus, dappId, cb)
      },
      function (cb) { // 更新mem_accounts表，根据受托人地址更新其得票信息，用于下一轮计算101排名
        self.getVotes(round, function (err, votes) {  // votes是回调函数的参数（受托人、票数的数组？），是self.getVotes的返回值
          if (err) {
            return cb(err);
          }
          async.eachSeries(votes, function (vote, cb) {
            var address = null
            if (global.featureSwitch.fixVoteNewAddressIssue) {
              address = modules.accounts.generateAddressByPublicKey2(vote.delegate)
            } else {
              address = modules.accounts.generateAddressByPublicKey(vote.delegate)
            }
            library.dbLite.query('update mem_accounts set vote = vote + $amount where address = $address', {  // 投票信息入库
              address: address,
              amount: vote.amount
            }, cb);
          }, function (err) { // eachSeries的回调函数
            library.bus.message('finishRound', round);  // 发送finishRound事件
            self.flush(round, function (err2) { // 删除本轮的mem_round表数据
              cb(err || err2);
            });
          })
        });
      }
    ], function (err) {
      delete private.feesByRound[round];  // 清理本轮变量数据，释放内存
      delete private.rewardsByRound[round];
      delete private.delegatesByRound[round];

      done(err);
    });
  });
}

Round.prototype.sandboxApi = function (call, args, cb) {  // 不明白
  sandboxHelper.callMethod(shared, call, args, cb);
}

// Events
Round.prototype.onBind = function (scope) { // 接收Bind事件
  modules = scope;
}

Round.prototype.onBlockchainReady = function () { // 接收BlockchainReady事件，更新私有变量的值
  var round = self.calc(modules.blocks.getLastBlock().height);
  library.dbLite.query("select sum(b.totalFee), GROUP_CONCAT(b.reward), GROUP_CONCAT(lower(hex(b.generatorPublicKey))) from blocks b where (select (cast(b.height / 101 as integer) + (case when b.height % 101 > 0 then 1 else 0 end))) = $round",
    {
      round: round
    },
    {
      fees: Number,
      rewards: Array,
      delegates: Array
    }, function (err, rows) {
      private.feesByRound[round] = rows[0].fees;
      private.rewardsByRound[round] = rows[0].rewards;
      private.delegatesByRound[round] = rows[0].delegates;
      private.loaded = true;
    });
}

Round.prototype.onFinishRound = function (round) {  // 接收finishRound事件
  library.network.io.sockets.emit('rounds/change', {number: round});  // 绑定事件？
}

Round.prototype.cleanup = function (cb) {
  private.loaded = false;
  cb();
}

// Shared

// Export
module.exports = Round; // 把一些方法暴露出去做接口
