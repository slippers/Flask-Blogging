try:
    from builtins import str
except ImportError:
    pass
import logging
from sqlalchemy import select, desc, func, and_, not_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from ..storage import Storage
from ..signals import sqla_initialized
from .models import (
    Post,
    Tag,
    Tag_Posts,
    User_Posts
)
from . import Base

class SQLAStorageModel(Storage):
    """
    The ``SQLAStorage`` implements the interface specified by the ``Storage``
    class. This  class uses SQLAlchemy to implement storage and retrieval of
    data from any of the databases supported by SQLAlchemy.
    """
    _logger = logging.getLogger("flask-blogging")

    def __init__(self, engine=None):
        """
        The constructor for the ``SQLAStorage`` class.
        :type table_prefix: str
        :param engine: The SQLAlchemy engine object
        """

        if engine is None:
            raise ValueError('engine is required')

        self._engine = engine

        Session = sessionmaker(bind=engine)

        self._session = Session()

        # the models have inherited Base, we have imported base from there.
        Base.metadata.create_all(engine)

    def save_post(self, title, text, user_id, tags, draft=False,
                  post_date=None, last_modified_date=None, meta_data=None,
                  post_id=None):
        """
        Persist the blog post data. If ``post_id`` is ``None`` or ``post_id``
        is invalid, the post must be inserted into the storage. If ``post_id``
        is a valid id, then the data must be updated.

        :param title: The title of the blog post
        :type title: str
        :param text: The text of the blog post
        :type text: str
        :param user_id: The user identifier
        :type user_id: str
        :param tags: A list of tags
        :type tags: list
        :param draft: (Optional) If the post is a draft of if needs to be
         published. (default ``False``)
        :type draft: bool
        :param post_date: (Optional) The date the blog was posted (default
         datetime.datetime.utcnow() )
        :type post_date: datetime.datetime
        :param last_modified_date: (Optional) The date when blog was last
         modified  (default datetime.datetime.utcnow() )
        :type last_modified_date: datetime.datetime
        :param post_id: (Optional) The post identifier. This should be ``None``
         for an insert call,
         and a valid value for update. (default ``None``)
        :type post_id: int
        :return: The post_id  value, in case of a successful insert or update.
         Return ``None`` if there were errors.
        """

        tags = self.normalize_tags(tags)

        try:
            # validate post_id
            post = self._session.query(Post).filter(Post.id==post_id).one_or_none()

            post_id_exists = post is not None

            if not post_id_exists:
                post = Post(title, text, draft, post_date, last_modified_date)
                self._session.add(post)
            else:
                post.update(title, text, draft, post_date, last_modified_date)

            self._session.commit()
            post_id = post.id
            self._save_tags(tags)
            self._save_tag_posts(tags, post_id)
            self._save_user_post(user_id, post_id)
        except Exception as e:
            self._logger.exception(str(e))
            post_id = None
            self._session.rollback()
            raise

        return post_id

    def get_post_by_id(self, post_id):
        """
        Fetch the blog post given by ``post_id``

        :param post_id: The post identifier for the blog post
        :type post_id: int
        :return: If the ``post_id`` is valid, the post data is retrieved, else
          returns ``None``.
        """

        r = None

        post = self._session.query(Post).filter(Post.id == post_id).one_or_none()

        if not post:
            return r

        r = dict(post_id=post.id,
                 title=post.title,
                 text=post.text,
                 post_date=post.post_date,
                 last_modified_date=post.last_modified_date,
                 draft=post.draft)

        r["tags"] = [tag_posts.tag.text for tag_posts in post.tag_posts]

        r["user_id"] = post.user_posts.user_id

        return r

    def get_posts(self, count=10, offset=0, recent=True, tag=None,
                  user_id=None, include_draft=False):
        """
        Get posts given by filter criteria

        :param count: The number of posts to retrieve (default 10)
        :type count: int
        :param offset: The number of posts to offset (default 0)
        :type offset: int
        :param recent: Order by recent posts or not
        :type recent: bool
        :param tag: Filter by a specific tag
        :type tag: str
        :param user_id: Filter by a specific user
        :type user_id: str
        :param include_draft: Whether to include posts marked as draft or not
        :type include_draft: bool

        :return: A list of posts, with each element a dict containing values
         for the following keys: (title, text, draft, post_date,
         last_modified_date). If count is ``None``, then all the posts are
         returned.
        """
        result = []
        ordering = desc(Post.post_date) if recent \
            else Post.post_date
        user_id = str(user_id) if user_id else user_id

        posts =  self._session.query(Post.id)

        posts = posts.order_by(ordering)

        sql_filter = self._get_filter(tag, user_id, include_draft)

        if sql_filter is not None:
            posts = posts.filter(sql_filter)
        if count:
            posts = posts.limit(count)
        if offset:
            posts = posts.offset(offset)

        try:
            result = posts.all()
        except Exception as e:
            self._logger.exception(str(e))

        posts = [self.get_post_by_id(pid[0]) for pid in result]
        return posts

    def count_posts(self, tag=None, user_id=None, include_draft=False):
        """
        Returns the total number of posts for the give filter

        :param tag: Filter by a specific tag
        :type tag: str
        :param user_id: Filter by a specific user
        :type user_id: str
        :param include_draft: Whether to include posts marked as draft or not
        :type include_draft: bool
        :return: The number of posts for the given filter.
        """
        xray = self._session.query(Post, func.count())

        result = 0
        with self._engine.begin() as conn:
            try:
                count_statement = select([func.count()]). \
                    select_from(Post)
                sql_filter = self._get_filter(tag, user_id, include_draft)
                count_statement = count_statement.where(sql_filter)
                result = conn.execute(count_statement).scalar()
            except Exception as e:
                self._logger.exception(str(e))
                result = 0
        return result

    def delete_post(self, post_id):
        """
        Delete the post defined by ``post_id``

        :param post_id: The identifier corresponding to a post
        :type post_id: int
        :return: Returns True if the post was successfully deleted and False
         otherwise.
        """
        try:
            post = self._session.query(Post).filter(Post.id == post_id).one_or_none()
            if post:
                self._session.delete(post)
                self._session.commit()
                return True
            else:
                return False

        except Exception as e:
            self._logger.exception(str(e))
            return False

    def _get_filter(self, tag, user_id, include_draft):
        filters = []
        if tag:
            tag = tag.upper()
            current_tag = self._session.query(Tag).filter(Tag.text == tag).one()

            if current_tag:
                tag_id = current_tag.id
                tag_filter = and_(
                    Tag_Posts.tag_id == tag_id,
                    Post.id == Tag_Posts.post_id
                )
                filters.append(tag_filter)

        if user_id:
            user_filter = and_(
                User_Posts.user_id == user_id,
                Post.id == User_Posts.post_id

            )
            filters.append(user_filter)

        draft_filter = Post.draft == 1 if include_draft else \
            Post.draft == 0
        filters.append(draft_filter)
        sql_filter = and_(*filters)

        return sql_filter

    def _save_tags(self, tags):

        tag_ids = []

        # get Tag for tags already stored
        current = self._session.query(Tag).filter(Tag.text.in_(tags)).all()
        current_tags = [tag.text for tag in current]

        # subtract tags current from tags that are new. 
        new_tags = set(tags) - set(current_tags)

        # store tags that were not found
        tag_objects = [Tag(tag) for tag in new_tags]

        if not tag_objects:
            return

        try:
            self._session.bulk_save_objects(tag_objects)
            self._session.commit()
        except IntegrityError as e:
            # some database error occurred;
            self._logger.exception(str(e))
        except Exception as e:
            # unknown exception occurred
            self._logger.exception(str(e))

    def _save_tag_posts(self, tags, post_id):
        # get tag records
        tag_result = self._session.query(Tag).filter(Tag.text.in_(tags)).all()
        tag_ids = [tag.id for tag in tag_result]

        # get Tag_Posts for current post_id
        tag_posts_result = self._session.query(Tag_Posts) \
                .filter(Tag_Posts.post_id == post_id).all()
        tag_post_ids = [tag_post.tag_id for tag_post in tag_posts_result]

        # delete tag_posts
        delete_tag_posts = set(tag_post_ids) - set(tag_ids)

        # new tag_posts
        new_tag_posts = set(tag_ids) - set(tag_post_ids)
        
        # perform delete and insert
        try:
            if delete_tag_posts:
               self._session.query(Tag_Posts) \
                        .filter(Tag_Posts.post_id==post_id) \
                        .filter(Tag_Posts.tag_id.in_(list(delete_tag_posts))) \
                        .delete(synchronize_session=False)
            if new_tag_posts:
                tag_posts = [Tag_Posts(tag_id=tag, post_id=post_id) for tag in new_tag_posts]
                self._session.bulk_save_objects(tag_posts)
            self._session.commit()
        except IntegrityError as e:
            # some database error occurred;
            self._logger.exception(str(e))
        except Exception as e:
            # unknown exception occurred
            self._logger.exception(str(e))

    def _save_user_post(self, user_id, post_id):
        user_id = str(user_id)

        try:
            user_posts = self._session.query(User_Posts) \
                    .filter(User_Posts.post_id == post_id) \
                    .one_or_none()

            if not user_posts:
                new_user_posts = User_Posts(user_id=user_id, post_id=post_id)
                self._session.add(new_user_posts)
            else:
                user_posts.update(user_id)
            self._session.commit()
        except IntegrityError as e:
            # some database error occurred;
            self._logger.exception(str(e))
        except Exception as e:
            # unknown exception occurred
            self._logger.exception(str(e))
