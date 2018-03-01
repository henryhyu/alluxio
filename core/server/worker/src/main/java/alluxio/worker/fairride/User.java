package alluxio.worker.fairride;

import alluxio.Configuration;
import alluxio.PropertyKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Iterator;
import java.util.Set;
import java.util.List;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.Collections;
import java.util.LinkedHashMap;

import java.util.concurrent.ConcurrentHashMap;

/**
* Represents a single user of a FairRide cache.
*
* The static methods are regrettably necessary to record when a
* block is cached, accessed, or removed, because the request
* options are not passed to the evictor that is acting as an
* event listener.
*/
public final class User implements Comparable<User> {
  private static final Logger LOG = LoggerFactory.getLogger(User.class);

  /**Maps user ID to user.*/
  private static Map<String, User> sUsers =  new ConcurrentHashMap<>();
  /**Maps block ID to the users who are caching it.*/
  private static Map<Long, HashSet<String>> sBlockIdsToUsers = new ConcurrentHashMap<>();
  /**Maps block ID to the block size.*/
  private static Map<Long, Long> sBlockIdsToSizes =  new ConcurrentHashMap<>();
  //TODO(caitscarberry): verify whether blocks are immutable

  private static final long BUDGET =
      Configuration.getBytes(PropertyKey.WORKER_MEMORY_SIZE) / 2;

  private static final boolean DO_BLOCKING =
      Configuration.getBoolean(PropertyKey.FAIRRIDE_BLOCKING_ON);

  private static final double DISK_BANDWIDTH =
      Configuration.getDouble(PropertyKey.FAIRRIDE_DISK_BANDWIDTH);

  /**This user's ID.*/
  private String mId;
  /**The blocks this user has cached.*/
  private Set<Long> mBlocksCached = new HashSet<>();

  private Map<Long, Double> mBlocksToPriority = new ConcurrentHashMap<>();

  private User(String id) {
    mId = id;
    User.sUsers.put(id, this);
  }

  /**
   * Resets all information about users and cached blocks.
   * ONLY TO BE USED IN TESTING.
   */
  public static void reset() {
    sUsers =  new ConcurrentHashMap<>();
    sBlockIdsToUsers =  new ConcurrentHashMap<>();
    sBlockIdsToSizes =  new ConcurrentHashMap<>();
  }

  /**
   * Actions when a user caches a block.
   *
   * @param userId User ID of the user who is caching this block
   * @param blockId the id of the block to access
   * @param blockSize the size of the block in bytes
   */
  public static void onUserCacheBlock(String userId, long blockId, long blockSize) {
    //Create user if one does not exist
    User u = getOrCreateUser(userId);

    u.mBlocksCached.add(blockId);

    if (!sBlockIdsToUsers.containsKey(blockId)) {
      sBlockIdsToUsers.put(blockId, new HashSet<String>());
    }
    sBlockIdsToUsers.get(blockId).add(userId);

    if (!sBlockIdsToSizes.containsKey(blockId)) {
      sBlockIdsToSizes.put(blockId, blockSize);
    }

    for (long otherBlockId : u.getCachedBlocksByPriority()) {
      if (u.getCost() <= BUDGET) {
        break;
      }

      //If this block is putting us over budget and
      //other users are caching it, stop caching it
      if (sBlockIdsToUsers.get(otherBlockId).size() > 1) {
        sBlockIdsToUsers.get(otherBlockId).remove(userId);
        u.mBlocksCached.remove(otherBlockId);
      }
    }
  }

  /**
   * Actions when a user accesses an already-cached block. Note that
   * the block does NOT need to be in this user's mBlocksCached to
   * access it.
   *
   * @param userId User ID of the user who is caching this block
   * @param blockId Block ID of block that is being cached
   */
  public static void onUserAccessBlock(String userId, long blockId) {
    //In practice, the block will always be cached before
    //this is called, but some tests break that assumption.
    //This check just allows them to pass.
    if (!sBlockIdsToUsers.containsKey(blockId) || !sBlockIdsToSizes.containsKey(blockId)) {
      return;
    }

    User u = getOrCreateUser(userId);

    u.incrementBlockPriority(blockId);

    if (!u.mBlocksCached.contains(blockId)) {
      //expected delaying (see section 3.4 of FairRide paper)
      double diskDelay = sBlockIdsToSizes.get(blockId)
          / ((double) (DISK_BANDWIDTH * 1048576 * .001));
      double pBlock = 1.0 / ((double) (sBlockIdsToUsers.get(blockId).size() + 1));

      if (DO_BLOCKING) {
        try {
          Thread.sleep((int) (diskDelay * pBlock));
        } catch (InterruptedException e) {
          LOG.warn("Delay failed");
        }
      }

      //after the delay, cache the file for the user
      u.onUserCacheBlock(userId, blockId, sBlockIdsToSizes.get(blockId));
    }
  }

  /**
   * Actions when a block is removed from the cache.
   *
   * @param blockId Block ID of block that is being removed from cache
   */
  public static void onBlockRemoved(long blockId) {
    sBlockIdsToSizes.remove(blockId);

    //If no users have cached the block, our work is done
    if (!sBlockIdsToUsers.containsKey(blockId)) {
      return;
    }

    //We _intentionally_ leave the priority untouched
    //because we want to keep a running count of how many times
    //the file has been accessed by each user

    for (String userId : sBlockIdsToUsers.get(blockId)) {
      User u = sUsers.get(userId);
      u.mBlocksCached.remove(blockId);
    }

    sBlockIdsToUsers.remove(blockId);
  }

  /**
   * Returns an iterator for the cached blocks in order of (highest user cost,
   * lowest block priority).
   *
   * @return an iterator over the ids of the blocks in
   * order of (highest user cost, lowest block priority)
   */
  public static Iterator<Long> getBlockIterator() {
    Set<Long> blocksSeen = new HashSet<Long>();
    List<Long> blocksToEvict = new ArrayList<>();

    List<User> userObjects = new ArrayList<>(sUsers.values());
    Collections.sort(userObjects);

    for (User u : userObjects) {
      blocksToEvict.addAll(u.getCachedBlocksByPriority());
    }

    blocksToEvict = blocksToEvict.stream()
     .distinct()
     .collect(Collectors.toList());

    return blocksToEvict.iterator();
  }

  /**
   * Returns an iterator for the cached blocks in order of (highest user cost,
   * lowest block priority) up to a total length of x bytes. This is a
   * convencience method to keep us from having to iterate through every
   * cached block.
   *
   * @param x the minimum total size of the blocks to get
   *
   * @return an iterator over the ids of the blocks in order of
   * (highest user cost, lowest block priority) up to x bytes
   */
  public static Iterator<Long> getBlockIteratorUpToXBytes(long x) {
    long bytesSeen = 0;
    List<Long> blocksToEvict = new ArrayList<>();

    List<User> userObjects = new ArrayList<>(sUsers.values());
    Collections.sort(userObjects);

    for (User u : userObjects) {
      Set<Long> blocksByPriority = u.getCachedBlocksByPriority();
      blocksToEvict.addAll(blocksByPriority);
      for (long blockId : blocksByPriority) {
        bytesSeen += sBlockIdsToSizes.get(blockId);
      }
      if (bytesSeen >= x) {
        break;
      }
    }

    blocksToEvict = blocksToEvict.stream()
     .distinct()
     .collect(Collectors.toList());

    return blocksToEvict.iterator();
  }

  /**
   * Returns whether or not a block should be cached by a given user.
   * This function MUST be called before a block is cached, to guarantee
   * that we follow the max-min fairness algorithm.
   *
   * @param userId User ID of user who wants to cache block
   * @param blockId Block ID of block that might be cached
   * @param blockSize size in bytes of the block to be cached
   * @param remainingCapacity size in bytes of space remaining in cache
   *
   * @return an iterator over the ids of the blocks in order of
   * (highest user cost, lowest block priority)
   */
  public static boolean shouldCacheBlock(
        String userId,
        long blockId,
        long blockSize,
        long remainingCapacity
  ) {
    User u  = getOrCreateUser(userId);
    double priorityOfNewBlock = u.getBlockPriority(blockId);

    //Don't cache when it would evict a higher-priority block
    //from the user's personal cache
    long blockCost = blockSize;
    if (sBlockIdsToUsers.containsKey(blockId)) {
      blockCost /= (sBlockIdsToUsers.get(blockId).size() + 1);
    }
    if (u.getCost() + blockCost > BUDGET && u.getLowestBlockPriority() > priorityOfNewBlock) {
      return false;
    }

    //Don't cache when it would evict a block cached by the same user
    //with a higher priority
    long willNeedToEvict = blockSize - remainingCapacity;
    if (willNeedToEvict <= 0) {
      return true;
    }
    return !u.fewerThanXBytesUntilUserPriorityBlock(willNeedToEvict, priorityOfNewBlock);
  }

  /**
   * Increments the priority of a certain block for a certain user.
   *
   * @param blockId ID of block
   * @param userId ID of user
   */
  public static void incrementBlockPriorityForUser(long blockId, String userId) {
    User u = getOrCreateUser(userId);
    u.incrementBlockPriority(blockId);
  }

  /**
   * Returns the user with ID userId. If a user with that ID does not
   * exist, creates a new one.
   *
   * @param userId ID of the user to get or create
   *
   * @return user with ID userId
   */
  private static User getOrCreateUser(String userId) {
    return (User.sUsers.containsKey(userId)) ? User.sUsers.get(userId) : new User(userId);
  }

  /**
   * Get the cost for this user.
   *
   * @return cost of user
   */
  public double getCost() {
    double cost = 0;
    for (Long blockId : mBlocksCached) {
      double usersCachingBlock = (double) sBlockIdsToUsers.get(blockId).size();
      cost += (double) sBlockIdsToSizes.get(blockId) / usersCachingBlock;
    }
    return cost;
  }

  /**
   * Get the ID for this user.
   *
   * @return ID of user
   */
  public String getId() {
    return mId;
  }

  /**
   * Compares users by descending cost, then by lexicographic order of user ID.
   * Because user IDs are unique, this ensures a total order.
   *
   * @param u the user to compare to
   *
   * @return 1 if u has lower cost, -1 if u has higher cost, and
   * 0 if costs are equal
   */
  public int compareTo(User u) {
    return (Double.compare(getCost(), u.getCost()) != 0)
      ? Double.compare(getCost(), u.getCost()) * (-1)
      : getId().compareTo(u.getId());
  }

  /**
   * Increments the priority of the block only for this user.
   *
   * @param blockId block ID of block to increment priority of
   */
  private void incrementBlockPriority(long blockId) {
    if (!mBlocksToPriority.containsKey(blockId)) {
      mBlocksToPriority.put(blockId, Double.valueOf(0));
    }

    //Cache is a simple LFU, so priority is number of accesses
    mBlocksToPriority.put(blockId, mBlocksToPriority.get(blockId) + 1);
  }

  /**
   * A set of the user's cached blocks in order of increasing priority.
   *
   * @return user's cached blocks in order of increasing priority
   */
  private Set<Long> getCachedBlocksByPriority() {
    return mBlocksToPriority.entrySet().stream()
      .filter(p -> mBlocksCached.contains(p.getKey()))
      .sorted(Map.Entry.<Long, Double>comparingByValue())
      .collect(Collectors.toMap(Map.Entry::getKey,
                     Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new))
      .keySet();
  }

  /**
   * Get the minimum priority value from the user's personal cache.
   *
   * @return priority of lowest-priority block from the user's personal cache
   * or -1 if there is none
   */
  private double getLowestBlockPriority() {
    double minPriority = -1;

    for (Long blockId : mBlocksCached) {
      double priority = getBlockPriority(blockId);
      if (priority < minPriority) {
        minPriority = priority;
      }
    }
    return minPriority;
  }

  /**
   * Returns priority of this block for this user. If user has never
   * accessed this block, priority is 1.
   *
   * @param blockId the block to get the priority of
   *
   * @return priority of block
   */
  public double getBlockPriority(long blockId) {
    return mBlocksToPriority.getOrDefault(blockId, (double) 1);
  }

  /**
   * Returns whether or not we can evict x bytes from the cache without evicting
   * a block which this user gives higher priority.
   *
   * @param x the number of bytes we plan to evict
   * @param priority the highest priority block we are allowed to evict
   *
   * @return whether or not we can evict x bytes from the cache without evicting
   * a block which this user gives priority greater than priority
   */
  private boolean fewerThanXBytesUntilUserPriorityBlock(long x, double priority) {
    int bytes = 0;
    Iterator<Long> blocks = getBlockIteratorUpToXBytes(x);

    while (blocks.hasNext() && bytes < x) {
      long blockId = blocks.next();
      if (getBlockPriority(blockId) > priority) {
        return true;
      }
      bytes += sBlockIdsToSizes.get(blockId);
    }

    return false;
  }
}
