import pickle
import random
import time
import uuid
import sys

import numpy

# import executor.redis_shim
# import retwis.retwis_lib

from anna.lattices import *

def run(flconn, kvs):


    class FluentRedisShim:
        def __init__(self, fluent_user_library):
            self._fluent_lib = fluent_user_library

        # Checking for existence of arbitrary keys.
        def exists(self, key):
            value_or_none = self._fluent_lib.get(key)
            return value_or_none is not None

        ## Single value storage.

        # Retrieving arbitrary values that were stored by set().
        def get(self, key):
            if self.exists(key):
                values = self._fluent_lib.get(key)
                value = values[0]
                value = pickle.loads(value)
                return value
            else:
                return None

        # Storing arbitary values that can be retrieved by get().
        def set(self, key, value):
            value = pickle.dumps(value)
            self._fluent_lib.put(key, value)

        ## Counter storage.

        # incr in Redis is used for two things:
        # - returning unique IDs
        # - as a counter
        # This mocks out the former.
        def incr(self, key):
            return int(uuid.uuid4())


        ## Set storage.

        # Add an item to the set at this key.
        def sadd(self, key, value):
            value = pickle.dumps(value)
            self._fluent_lib.put(key, value)

        # Remove an item from the set at this key.
        def srem(self, key, value):
            raise NotImplementedError  # No removals in experiments rn; implement tombstones later if you need it.

        # Set contents.
        def smembers(self, key):
            if self.exists(key):
                values = self._fluent_lib.get(key)
                return set((pickle.loads(val) for val in values))
            else:
                return set()

        # Set membership.
        def sismember(self, key):
            return key in self.smembers(key)

        # Set size.
        def scard(self, key):
            return len(self.smembers(key))


        ## Append-only lists.

        # Append.
        def lpush(self, key, value):
            # microseconds.
            # This value will be 16 digits long for the foreseeable future.
            ts = int(time.time() * 1000000)
            value = pickle.dumps(value)
            value = ('{}:{}'.format(ts, value)).encode()
            self._fluent_lib.put(key, value)

        # Slice.
        def lrange(self, key, begin, end):
            if self.exists(key):
                values = self._fluent_lib.get(key)
                oset = ListBasedOrderedSet(values)
                values = [
                    # trim off timestamp + delimiter, and deserialize the rest.
                    pickle.loads(eval(item.decode()[17:]))
                    for item in oset.lst[begin:end]
                ]
                return values
            else:
                return []

        # Size.
        def llen(self, key):
            if self.exists(key):
                values = self._fluent_lib.get(key)
                return len(list(values))
            else:
                return 0

    class Timeline:
      @staticmethod
      def page(r,page):
        _from = (page-1)*10
        _to = (page)*10
        return [Post(r, post_id).content for post_id in r.lrange('timeline',_from,_to)]

    class Model(object):
      def __init__(self, r, id):
        self.__dict__['id'] = id
        self.__dict__['r'] = r

      def __eq__(self,other):
        return self.id == other.id

      def __setattr__(self,name,value):
        if name not in self.__dict__:
          klass = self.__class__.__name__.lower()
          key = '%s:id:%s:%s' % (klass,self.id,name.lower())
          self.r.set(key,value)
        else:
          self.__dict__[name] = value

      def __getattr__(self,name):
        if name not in self.__dict__:
          klass = self.__class__.__name__.lower()
          v = self.r.get('%s:id:%s:%s' % (klass,self.id,name.lower()))
          if v:
            return v
          raise AttributeError('%s doesn\'t exist' % name)
        else:
          return self.__dict__[name]

    class User(Model):
      @staticmethod
      def find_by_user(r, user):
        _id = r.get("user:user:%s" % user)
        if _id is not None:
          return int(_id)
        else:
          return None

      @staticmethod
      def find_by_id(_id):
        if r.exists("user:id:%s:user" % _id):
          return User(int(_id))
        else:
          return None

      @staticmethod
      def create(r, user, password):
        user_id = r.incr("user:uid")
        # if not r.get("user:user:%s" % user):  # XXX existence checking not implemented rn
        r.set("user:id:%s:user" % user_id, user)
        r.set("user:user:%s" % user, user_id)

        r.set("user:id:%s:password" % user_id, password)
        r.lpush("users", user_id)
        # return User(user_id)
        # return None

      def posts(self,page=1):
        _from, _to = (page-1)*10, page*10
        posts = r.lrange("user:id:%s:posts" % self.id, _from, _to)
        if posts:
          return [Post(int(post_id)) for post_id in posts]
        return []

      @staticmethod
      def timeline(r, user, page=1):
        userid = User.find_by_user(r, user)
        timeline_len = r.llen("user:id:%s:timeline" % userid)
        _from, _to = timeline_len - page*10, timeline_len - (page-1)*10,
        timeline = r.lrange("user:id:%s:timeline" % userid, _from, _to)
        if timeline:
          return [Post(r, int(post_id)).content for post_id in timeline]
        return []

      def mentions(self,page=1):
        _from, _to = (page-1)*10, page*10
        mentions = r.lrange("user:id:%s:mentions" % self.id, _from, _to)
        if mentions:
          return [Post(int(post_id)) for post_id in mentions]
        return []

      @staticmethod
      def add_post(r, userid, post_id):
        r.lpush("user:id:%s:posts" % userid, post_id)
        r.lpush("user:id:%s:timeline" % userid, post_id)
        r.sadd('posts:id', post_id)

      @staticmethod
      def add_timeline_post(r, userid, post_id):
        r.lpush("user:id:%s:timeline" % userid, post_id)

      def add_mention(self,post):
        r.lpush("user:id:%s:mentions" % self.id, post.id)

      @staticmethod
      def follow(r, user, target):
        userid = User.find_by_user(r, user)
        targetid = User.find_by_user(r, target)
        if userid == targetid:
          return
        else:
          r.sadd("user:id:%s:followees" % userid, targetid)
          User.add_follower(r, targetid, userid)

      def stop_following(self,user):
        r.srem("user:id:%s:followees" % self.id, user.id)
        user.remove_follower(self)

      def following(self,user):
        if r.sismember("user:id:%s:followees" % self.id, user.id):
          return True
        return False

      @staticmethod
      def followers(r, userid):
        followers = r.smembers("user:id:%s:followers" % userid)
        if followers:
          return followers
        return []

      @staticmethod
      def followees(userid):
        followees = r.smembers("user:id:%s:followees" % userid)
        if followees:
          return followers
        return []


      #added
      @property
      def tweet_count(self):
        return r.llen("user:id:%s:posts" % self.id) or 0

      @property
      def followees_count(self):
        return r.scard("user:id:%s:followees" % self.id) or 0

      @property
      def followers_count(self):
        return r.scard("user:id:%s:followers" % self.id) or 0

      @staticmethod
      def add_follower(r, userid, targetid):
        r.sadd("user:id:%s:followers" % userid, targetid)

      def remove_follower(self,user):
        r.srem("user:id:%s:followers" % self.id, user.id)

    class Post(Model):
      @staticmethod
      def create(r, user, content):
        userid = User.find_by_user(r, user)
        post_id = r.incr("post:uid")
        post = Post(r, post_id)
        post.content = content
        post.user_id = userid
        # #post.created_at = Time.now.to_s
        User.add_post(r, userid, post_id)
        # r.lpush("timeline", post_id)  # not testing global timeline
        for follower in User.followers(r, userid):
          User.add_timeline_post(r, follower, post_id)

        # mentions = re.findall('@\w+', content)
        # for mention in mentions:
        #   u = User.find_by_user(mention[1:])
        #   if u:
        #     u.add_mention(post)

      @staticmethod
      def find_by_id(id):
        if r.sismember('posts:id', int(id)):
          return Post(id)
        return None

      @property
      def user(self):
        return User.find_by_id(r.get("post:id:%s:user_id" % self.id))




    def aaa_redis_exists(fluent, key):
        redis = FluentRedisShim(fluent)
        return str(redis.exists(key))
    def aaa_redis_get(fluent, key):
        redis = FluentRedisShim(fluent)
        return str(redis.get(key))
    def aaa_redis_set(fluent, key, value):
        redis = FluentRedisShim(fluent)
        redis.set(key, value)
        return 'success'
    def aaa_redis_incr(fluent, key):
        redis = FluentRedisShim(fluent)
        return str(redis.incr(key))
    def aaa_redis_sadd(fluent, key, value):
        redis = FluentRedisShim(fluent)
        redis.sadd(key, value)
        return 'success'
    def aaa_redis_smembers(fluent, key):
        redis = FluentRedisShim(fluent)
        return str(redis.smembers(key))
    def aaa_redis_lpush(fluent, key, value):
        redis = FluentRedisShim(fluent)
        redis.lpush(key, value)
        return 'success'
    def aaa_redis_lrange(fluent, key, begin, end):
        redis = FluentRedisShim(fluent)
        return str(redis.lrange(key, begin, end))
    def aaa_redis_llen(fluent, key):
        redis = FluentRedisShim(fluent)
        return str(redis.llen(key))




    # def aaa_global_timeline(fluent_lib, page):
    #     f_redis = FluentRedisShim(fluent_lib)
    #     return Timeline.page(f_redis, page)

    def aaa_user_create(fluent_lib, user):
        f_redis = FluentRedisShim(fluent_lib)
        # fluent_lib.put('aaa_user', SetLattice({b'hi',}))
        # fluent_lib.put('aaa_userb', OrderedSetLattice(ListBasedOrderedSet([b'hi'])))
        User.create(f_redis, user, 'password')
        return 'success'

    def aaa_user_timeline(fluent_lib, user, page):
        f_redis = FluentRedisShim(fluent_lib)
        return User.timeline(f_redis, user, page)

    def aaa_user_profile(fluent_lib, user, page):
        pass

    def aaa_user_follow(fluent_lib, user, target):
        f_redis = FluentRedisShim(fluent_lib)
        User.follow(f_redis, user, target)
        return 'success'

    def aaa_post_create(fluent_lib, user, post):
        f_redis = FluentRedisShim(fluent_lib)
        Post.create(f_redis, user, post)
        return 'success'

    # user, postcontent, pids -> ...
    # def post_create_with_dep(flib, user, post, deps):
    #     pass

    # dag_name = 'read-and-tweet'
    # ['user_timeline_pids', 'post_create_with_dep']


    fns = {
        'aaa_redis_exists': aaa_redis_exists,
        'aaa_redis_get': aaa_redis_get,
        'aaa_redis_set': aaa_redis_set,
        'aaa_redis_incr': aaa_redis_incr,
        'aaa_redis_sadd': aaa_redis_sadd,
        'aaa_redis_smembers': aaa_redis_smembers,
        'aaa_redis_lpush': aaa_redis_lpush,
        'aaa_redis_lrange': aaa_redis_lrange,
        'aaa_redis_llen': aaa_redis_llen,
        # 'aaa_global_timeline': aaa_global_timeline,
        'aaa_user_create': aaa_user_create,
        'aaa_user_timeline': aaa_user_timeline,
        'aaa_user_profile': aaa_user_profile,
        'aaa_user_follow': aaa_user_follow,
        'aaa_post_create': aaa_post_create,
    }

    cfns = {
        fname: flconn.register(f, fname)
        for fname, f
        in fns.items()
    }

    for fname, cf in cfns.items():
        if cf:
            print ("Successfully registered {}.".format(fname))

    def callfn(fname, *args):
        r = cfns[fname](*args).get()
        print("%s(%s) -> %s" % (fname, args, r))

    # Redis shim tests (not retwis related).
    callfn('aaa_redis_exists', 'aaa_foo')
    callfn('aaa_redis_set', 'aaa_foo', b'3')
    callfn('aaa_redis_get', 'aaa_foo')
    callfn('aaa_redis_incr', 'aaa_cntr')
    callfn('aaa_redis_sadd', 'aaa_sxt', b'4')
    callfn('aaa_redis_smembers', 'aaa_sxt')
    callfn('aaa_redis_lpush', 'aaa_lxt', b'5')
    callfn('aaa_redis_lrange', 'aaa_lxt', 0, 10)
    callfn('aaa_redis_llen', 'aaa_lxt')
    callfn('aaa_redis_lpush', 'aaa_lxt', b'6')
    callfn('aaa_redis_lpush', 'aaa_lxt', b'4')
    callfn('aaa_redis_lrange', 'aaa_lxt', 0, 10)

    # Experiment parameters.
    num_users = 10
    max_degree = 3
    num_pretweets = 100
    num_ops = 100  # 80% reads, 20% writes
    usernames = [str(i + 1) for i in range(num_users)]

    # -> str

    def get_random_user():
        return str(int(random.random() * num_users) + 1)

    def get_zipf_user():
        a = 1.5  # "realistic social network distribution" from johann
        res = numpy.random.zipf(1.5)
        while res > num_users:
            res = numpy.random.zipf(1.5)
        return str(res)

    def get_n_zipf_users(n):
        users = set()
        while len(users) < n:
            users.add(get_zipf_user())
        return users


    print ("Making users...")
    # Make all the users.
    for username in usernames:
        res = cfns['aaa_user_create'](username).get()
        if res != 'success':
            print("aaa_user_create(%s) -> %s" % (username, str(res)))
            sys.exit(1)

    # Make all the user connections.
    # Every user calls follow max_degree times.
    # The people they follow are zipfian-distributed.

    print ("Making user connections...")
    for username in usernames:
        targets = get_n_zipf_users(max_degree)
        for target in targets:
            res = cfns['aaa_user_follow'](username, target).get()
            if res != 'success':
                print("aaa_user_follow(%s, %s) -> %s" % (username, target, str(res)))
                sys.exit(1)

    # Prepopulating tweets, so our read and write times are more realistic.
    print ("Prepopulating tweets...")
    for _ in range(num_pretweets):
        username = get_random_user()
        post = "{} says: I love fluent!".format(username)
        res = cfns['aaa_post_create'](username, post).get()

    # Execute workload.
    rtimes = []
    wtimes = []
    start = time.time()
    print ("Executing workload...")
    for numop in range(num_ops):
        t = random.random()
        # Pick a user at uniform.
        username = get_random_user()
        # 80% reads.
        if t < 0.8:
            r_start = time.time()
            res = cfns['aaa_user_timeline'](username, 1).get()
            rtimes.append(time.time() - r_start)

        # 20% writes.
        else:
            w_start = time.time()
            post = "{} says: I LOVE fluent!".format(username)
            res = cfns['aaa_post_create'](username, post).get()
            wtimes.append(time.time() - w_start)

    end = time.time()
    elapsed = end - start


    # Sanity check: print timeline of most and least popular user.
    res = cfns['aaa_user_timeline']('1', 1).get()
    print("aaa_user_timeline('1', 1) -> %s" % (str(res)))
    res = cfns['aaa_user_timeline'](str(num_users), 1).get()
    print("aaa_user_timeline(%s, 1) -> %s" % (str(num_users), str(res)))





    # res = cfns['aaa_user_create']('bobaaa_').get()
    # print("aaa_user_create('bobaaa_') -> %s" % (str(res)))
    # res = cfns['aaa_user_create']('emilyaaa_').get()
    # print("aaa_user_create('emilyaaa_') -> %s" % (str(res)))
    # res = cfns['aaa_user_follow']('emilyaaa_', 'bobaaa_').get()
    # print("aaa_user_follow('emilyaaa_', 'bobaaa_') -> %s" % (str(res)))
    # res = cfns['aaa_post_create']('bobaaa_', 'im bob lol').get()
    # print("aaa_post_create('bobaaa_', 'im bob lol') -> %s" % (str(res)))
    # res = cfns['aaa_post_create']('emilyaaa_', 'im emily lol').get()
    # print("aaa_post_create('emilyaaa_', 'im emily lol') -> %s" % (str(res)))
    # res = cfns['aaa_user_timeline']('bobaaa_', 1).get()
    # print("aaa_user_timeline('bobaaa_', 1) -> %s" % (str(res)))
    # res = cfns['aaa_user_timeline']('emilyaaa_', 1).get()
    # print("aaa_user_timeline('emilyaaa_', 1) -> %s" % (str(res)))




    return [elapsed], rtimes, wtimes, 0
