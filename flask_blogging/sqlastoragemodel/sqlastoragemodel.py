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
            if post_id is None:
                post = Post(title, text, draft, post_date, last_modified_date)
                self._session.add(post)
            else:
                post = Post.query.filter_by(id=post_id).first()
                if post is None:
                    raise ValueError('post id invalid.')
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
        with self._engine.begin() as conn:
            try:
                post_statement = sqla.select([self._post_table]).where(
                    self._post_table.c.id == post_id
                )
                post_result = conn.execute(post_statement).fetchone()
                if post_result is not None:
                    r = dict(post_id=post_result[0], title=post_result[1],
                             text=post_result[2], post_date=post_result[3],
                             last_modified_date=post_result[4],
                             draft=post_result[5])
                    # get the tags
                    tag_statement = sqla.select([self._tag_table.c.text]). \
                        where(
                            sqla.and_(
                                self._tag_table.c.id ==
                                self._tag_posts_table.c.tag_id,
                                self._tag_posts_table.c.post_id == post_id))
                    tag_result = conn.execute(tag_statement).fetchall()
                    r["tags"] = [t[0] for t in tag_result]
                    # get the user
                    user_statement = sqla.select([
                        self._user_posts_table.c.user_id]).where(
                        self._user_posts_table.c.post_id == post_id
                    )
                    user_result = conn.execute(user_statement).fetchone()
                    r["user_id"] = user_result[0]
            except Exception as e:
                self._logger.exception(str(e))
                r = None
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
        ordering = sqla.desc(self._post_table.c.post_date) if recent \
            else self._post_table.c.post_date
        user_id = str(user_id) if user_id else user_id

        with self._engine.begin() as conn:
            try:
                select_statement = sqla.select([self._post_table.c.id])
                sql_filter = self._get_filter(tag, user_id, include_draft,
                                              conn)

                if sql_filter is not None:
                    select_statement = select_statement.where(sql_filter)
                if count:
                    select_statement = select_statement.limit(count)
                if offset:
                    select_statement = select_statement.offset(offset)

                select_statement = select_statement.order_by(ordering)
                result = conn.execute(select_statement).fetchall()
            except Exception as e:
                self._logger.exception(str(e))
                result = []

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
        result = 0
        with self._engine.begin() as conn:
            try:
                count_statement = sqla.select([sqla.func.count()]). \
                    select_from(self._post_table)
                sql_filter = self._get_filter(tag, user_id, include_draft,
                                              conn)
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
        status = False
        success = 0
        with self._engine.begin() as conn:
            try:
                post_del_statement = self._post_table.delete().where(
                    self._post_table.c.id == post_id)
                conn.execute(post_del_statement)
                success += 1
            except Exception as e:
                self._logger.exception(str(e))
            try:
                user_posts_del_statement = self._user_posts_table.delete(). \
                    where(self._user_posts_table.c.post_id == post_id)
                conn.execute(user_posts_del_statement)
                success += 1
            except Exception as e:
                self._logger.exception(str(e))
            try:
                tag_posts_del_statement = self._tag_posts_table.delete(). \
                    where(self._tag_posts_table.c.post_id == post_id)
                conn.execute(tag_posts_del_statement)
                success += 1
            except Exception as e:
                self._logger.exception(str(e))
        status = success == 3
        return status

    def _get_filter(self, tag, user_id, include_draft, conn):
        filters = []
        if tag:
            tag = tag.upper()
            tag_statement = sqla.select([self._tag_table.c.id]).where(
                self._tag_table.c.text == tag)
            tag_result = conn.execute(tag_statement).fetchone()
            if tag_result is not None:
                tag_id = tag_result[0]
                tag_filter = sqla.and_(
                    self._tag_posts_table.c.tag_id == tag_id,
                    self._post_table.c.id == self._tag_posts_table.c.post_id
                )
                filters.append(tag_filter)

        if user_id:
            user_filter = sqla.and_(
                self._user_posts_table.c.user_id == user_id,
                self._post_table.c.id == self._user_posts_table.c.post_id
            )
            filters.append(user_filter)

        draft_filter = self._post_table.c.draft == 1 if include_draft else \
            self._post_table.c.draft == 0
        filters.append(draft_filter)
        sql_filter = sqla.and_(*filters)
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

        # get Tag_Posts for current post_id
        tag_posts_result = self._session.query(Tag_Posts) \
                .filter(Tag_Posts.post_id == post_id).all()

        # delete tag_posts
        tag_id_set = [tag.id for tag in tag_result]
        tag_post_id_set = [tag_post.tag_id for tag_post in tag_posts_result]
        delete_tag = set(tag_post_id_set) - set(tag_id_set)

        # new tag_posts
        new_tag_posts = []
        for tag in tag_result:
            for tag_post in tag_posts_result:
                if tag_post.tag_id == tag.id:
                    continue
            new_tag_posts.append(Tag_Posts(tag_id=tag.id, post_id=post_id))

        # perform delete and insert
        try:
            if delete_tag:
                self._session.query(Tag_Posts) \
                        .filter(Tag_Posts.post_id==post_id) \
                        .filter(Tag_Posts.tag_id.in_(delete_tag)) \
                        .delete()
            if new_tag_posts:
                self._session.bulk_save_objects(new_tag_posts)
            self._session.commit()
        except IntegrityError as e:
            # some database error occurred;
            self._logger.exception(str(e))
        except Exception as e:
            # unknown exception occurred
            self._logger.exception(str(e))

    def _save_user_post(self, user_id, post_id):
        user_id = str(user_id)
        user_posts = self._session.query(User_Posts) \
                .filter(User_Posts.post_id==post_id) \
                .first()
        try:
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
