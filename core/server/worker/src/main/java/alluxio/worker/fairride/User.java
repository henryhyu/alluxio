package alluxio.worker.fairride;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.Set;

public class User {
  private static final Logger LOG = LoggerFactory.getLogger(User.class);
  private static HashMap<String, User> users = new HashMap<String, User>();

  private String mId;

  public static void onUserCacheBlock(String id, long blockId) {
    //Because this block was just moved into the cache, this user is the
    //first one to cache it
    User u = (User.users.containsKey(id))? User.users.get(id) : new User(id);
    LOG.debug("User " + id + " cached block " + blockId);
  }

  public static void logUsers() {
    Set set = User.users.entrySet();
    Iterator iterator = set.iterator();
    LOG.warn("Printing users:");
    while(iterator.hasNext()) {
       Map.Entry user = (Map.Entry)iterator.next();
       LOG.warn(user.getKey().toString());
    }

  }

  private User(String id) {
    mId = id;
    User.users.put(id, this);
  }

}