CREATE TABLE public.network_user (
    collected_at timestamp with time zone not null,
    id bigint not null,
    screen_name text,
    name text,
    description text,
    language text,
    location text,
    created_at timestamp with time zone,
    followers_collected int,
    friends_collected int,
    followers_count int,
    friends_count int,
    statuses_count int,
    favorites_count int,
    verified boolean,
    raw jsonb not null,
    primary key (id, collected_at)
);


CREATE TABLE public.network_user_friend (
    collected_at timestamp with time zone not null,
    user_id bigint not null,
    friend_id bigint not null,
    primary key (user_id, friend_id, collected_at),
    foreign key (user_id, collected_at) references public.network_user (id, collected_at)
);


CREATE TABLE public.network_user_follower (
    collected_at timestamp with time zone not null,
    user_id bigint not null,
    follower_id bigint not null,
    primary key (user_id, follower_id, collected_at),
    foreign key (user_id, collected_at) references public.network_user (id, collected_at)
);


CREATE TABLE public.network_tweet (
    id bigint NOT NULL,
    created_at timestamp with time zone NOT NULL,
    tweet text NOT NULL,
    source text,
    language text,
    user_id bigint,
    user_screen_name text,
    in_reply_to_status_id bigint,
    in_reply_to_user_id bigint,
    in_reply_to_user_screen_name text,
    retweeted_status_id bigint,
    retweeted_status_user_id bigint,
    retweeted_status_user_screen_name text,
    retweeted_status_user_name text,
    retweeted_status_user_description text,
    retweeted_status_user_friends_count integer,
    retweeted_status_user_statuses_count integer,
    retweeted_status_user_followers_count integer,
    retweeted_status_retweet_count integer,
    retweeted_status_favorite_count integer,
    retweeted_status_reply_count integer,
    quoted_status_id bigint,
    quoted_status_user_id bigint,
    quoted_status_user_screen_name text,
    quoted_status_user_name text,
    quoted_status_user_description text,
    quoted_status_user_friends_count integer,
    quoted_status_user_statuses_count integer,
    quoted_status_user_followers_count integer,
    quoted_status_retweet_count integer,
    quoted_status_favorite_count integer,
    quoted_status_reply_count integer,
    hashtags text[],
    urls text[],
    raw jsonb not null,
    primary key (id)
);
