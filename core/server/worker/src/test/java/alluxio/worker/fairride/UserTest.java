package alluxio.worker.fairride;

import alluxio.worker.fairride.User;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;

public final class UserTest {
  @Before
  public void initialize() {
    User.reset();
  }

  @Test
  public void getBlockIterator() throws Exception {
    int[] blockSizes = {20, 10, 40};

    //Cache block 0 and make it priority 2 for user 1
    User.onUserCacheBlock("1", 0, blockSizes[0]);
    User.onUserAccessBlock("1", 0);

    //Cache block 1 and make it priority 1 for user 1
    User.onUserCacheBlock("1", 1, blockSizes[1]);

    //Block 1 should be nominated for eviction because it has lowest
    //priority
    Iterator<Long> blockIterator = User.getBlockIterator();
    Assert.assertTrue(blockIterator.next() == 1);

    //Cache block 2 for user 2 and make it priority 1
    User.onUserCacheBlock("2", 2, blockSizes[2]);

    //User 1 cost: 30, user 2 cost: 40
    //Block 2 should be nominated for eviction because user 2
    //has the highest cost
    blockIterator = User.getBlockIterator();
    Assert.assertTrue(blockIterator.next() == 2);

    //User 1 accesses block 2, which will cause block 2
    //to be cached for user 1. Now user 1 and user 2 share the
    //cost of block 2. Give block 2 priority 4 for user 1.
    User.onUserAccessBlock("1", 2);
    User.onUserAccessBlock("1", 2);
    User.onUserAccessBlock("1", 2);
    User.onUserAccessBlock("1", 2);

    //User 1 cost: 50, user 2 cost: 20
    //Block 1 should be nominated for eviction because it has
    //lowest priority for user 1
    blockIterator = User.getBlockIterator();
    Assert.assertTrue(blockIterator.next() == 1);

    //User 1 accesses block 1 twice, increasing its priority
    //to 3
    User.onUserAccessBlock("1", 1);
    User.onUserAccessBlock("1", 1);

    //Block 0 should be nominated for eviction because
    //it has lowest priority for user 1
    blockIterator = User.getBlockIterator();
    Assert.assertTrue(blockIterator.next() == 0);

    //After removing block 0, user 1 still has the
    //highest cost, and block 1 is its lowest-priority
    //block
    User.onBlockRemoved(0);
    blockIterator = User.getBlockIterator();
    Assert.assertTrue(blockIterator.next() == 1);

    //After removing block 1, user 1 still has the
    //highest cost, and block 2 is its lowest-priority
    //block
    User.onBlockRemoved(1);
    blockIterator = User.getBlockIterator();
    Assert.assertTrue(blockIterator.next() == 2);

    //After removing block 2, the cache is empty, so
    //no blocks should be nominated for eviction
    User.onBlockRemoved(2);
    blockIterator = User.getBlockIterator();
    Assert.assertTrue(!blockIterator.hasNext());
  }

  @Test
  public void shouldCacheBlock() throws Exception {
    int[] blockSizes = {20, 10, 40};

    //Cache block 0 and make it priority 2 for user 1
    User.onUserCacheBlock("1", 0, blockSizes[0]);
    User.onUserAccessBlock("1", 0);

    //If the remaining cache capacity is 5 bytes, caching
    //block 1 will evict block 0. Block 0 has a higher
    //priority for user 1 than block 1, so we shouldn't
    //cache it.
    Assert.assertTrue(!User.shouldCacheBlock("1", 1, blockSizes[1], 5));

    //User 2 doesn't care about user 1's block priorities
    Assert.assertTrue(User.shouldCacheBlock("2", 1, blockSizes[1], 5));

    //Cache block 0 and make it priority 2 for user 1.
    //User 2 now has the highest cost, so its blocks will be
    //nominated for eviction first.
    User.onUserCacheBlock("2", 2, blockSizes[2]);
    User.onUserAccessBlock("2", 2);

    //Block 2 will be nominated for eviction, but user 2 gives it higher
    //priority than block 1. Don't cache.
    Assert.assertTrue(!User.shouldCacheBlock("2", 1, blockSizes[1], 5));

    //User 1 doesn't care about user 2's block priorities
    Assert.assertTrue(User.shouldCacheBlock("1", 1, blockSizes[1], 5));
  }
}
