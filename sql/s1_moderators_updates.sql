drop table if exists s1_moderators_updates;

create table if not exists public.s1_moderators_updates
(
	id serial,
	subreddit text,
	moderator text,
	update_time timestamp without time zone 
);


insert into s1_moderators_updates(subreddit, update_time, moderator)
values 

('AnimalsFailing', '2014-05-09 16:53:00', 'Rusty5hackleford'),
('AnimalsFailing', '2014-06-01 08:24:00', 'Rusty5hackleford'),
('AnimalsFailing', '2014-07-06 09:36:00', 'Rusty5hackleford'),
('AnimalsFailing', '2014-08-03 12:00:00', 'Rusty5hackleford'),
('AnimalsFailing', '2014-09-07 14:25:00', 'Rusty5hackleford'),
('AnimalsFailing', '2014-10-05 17:16:00', 'Rusty5hackleford'),
('AnimalsFailing', '2014-11-11 02:43:00', 'Rusty5hackleford'),
('AnimalsFailing', '2014-12-02 04:18:00', 'Rusty5hackleford'),
('AnimalsFailing', '2015-01-06 07:42:00', 'Rusty5hackleford'),
('AnimalsFailing', '2015-02-04 13:24:00', 'Rusty5hackleford'),
('AnimalsFailing', '2015-03-02 12:03:00', 'Rusty5hackleford'),
('AnimalsFailing', '2015-04-07 03:18:00', 'Rusty5hackleford'),
('AnimalsFailing', '2015-06-02 23:29:00', 'Rusty5hackleford'),
('AnimalsFailing', '2015-07-03 02:36:00', 'Rusty5hackleford'),
('AnimalsFailing', '2015-08-13 13:40:00', 'Rusty5hackleford'),
('AnimalsFailing', '2015-10-23 01:56:00', 'Rusty5hackleford'),
('AnimalsFailing', '2015-11-13 02:32:00', 'Rusty5hackleford'),
('AnimalsFailing', '2015-12-04 10:11:00', 'Rusty5hackleford'),
('AnimalsFailing', '2016-01-01 11:33:00', 'Rusty5hackleford'),
('AnimalsFailing', '2016-02-01 23:26:00', 'Rusty5hackleford'),
('AnimalsFailing', '2016-03-01 04:06:00', 'Rusty5hackleford'),
('AnimalsFailing', '2016-04-05 15:13:00', 'Rusty5hackleford'),
('AnimalsFailing', '2016-05-17 07:51:00', 'Rusty5hackleford'),
('AnimalsFailing', '2016-06-07 11:20:00', 'Rusty5hackleford'),
('AnimalsFailing', '2016-07-19 23:13:00', 'Rusty5hackleford'),
('AnimalsFailing', '2016-09-09 12:33:00', 'Rusty5hackleford'),
('AnimalsFailing', '2016-10-15 06:05:00', 'Rusty5hackleford'),
('AnimalsFailing', '2016-12-22 12:37:00', 'Rusty5hackleford'),
('AnimalsFailing', '2016-12-22 12:37:00', 'IAMA_Plumber-AMA'),
('AnimalsFailing', '2017-01-05 10:27:00', 'Rusty5hackleford'),
('AnimalsFailing', '2017-01-05 10:27:00', 'IAMA_Plumber-AMA'),
('AnimalsFailing', '2017-04-08 01:24:00', 'Rusty5hackleford'),
('AnimalsFailing', '2017-04-08 01:24:00', 'IAMA_Plumber-AMA'),
('AnimalsFailing', '2018-03-11 12:04:00', 'Rusty5hackleford'),
('AnimalsFailing', '2018-03-11 12:04:00', 'IAMA_Plumber-AMA'),
('AnimalsFailing', '2018-07-18 16:41:00', 'Rusty5hackleford'),
('AnimalsFailing', '2018-07-18 16:41:00', 'IAMA_Plumber-AMA'),
('AskWomenOver30', '2013-09-24 04:07:00', 'ripster55'),
('AskWomenOver30', '2013-09-24 04:07:00', 'MonsieurJongleur'),
('AskWomenOver30', '2014-04-20 04:43:00', 'ripster55'),
('AskWomenOver30', '2014-04-20 04:43:00', 'MonsieurJongleur'),
('AskWomenOver30', '2014-04-20 04:43:00', 'AutoModerator'),
('AskWomenOver30', '2014-06-28 11:44:00', 'ripster55'),
('AskWomenOver30', '2014-06-28 11:44:00', 'MonsieurJongleur'),
('AskWomenOver30', '2014-06-28 11:44:00', 'AutoModerator'),
('AskWomenOver30', '2014-07-16 15:56:00', 'ripster55'),
('AskWomenOver30', '2014-07-16 15:56:00', 'MonsieurJongleur'),
('AskWomenOver30', '2014-07-16 15:56:00', 'AutoModerator'),
('AskWomenOver30', '2014-08-05 19:30:00', 'ripster55'),
('AskWomenOver30', '2014-08-05 19:30:00', 'MonsieurJongleur'),
('AskWomenOver30', '2014-08-05 19:30:00', 'AutoModerator'),
('AskWomenOver30', '2014-09-20 08:04:00', 'ripster55'),
('AskWomenOver30', '2014-09-20 08:04:00', 'MonsieurJongleur'),
('AskWomenOver30', '2014-09-20 08:04:00', 'AutoModerator'),
('AskWomenOver30', '2015-02-28 00:46:00', 'MonsieurJongleur'),
('AskWomenOver30', '2015-02-28 00:46:00', 'AutoModerator'),
('AskWomenOver30', '2015-08-18 03:39:00', 'MonsieurJongleur'),
('AskWomenOver30', '2015-08-18 03:39:00', 'MoeBetterBooze'),
('AskWomenOver30', '2017-04-24 10:53:00', 'MonsieurJongleur'),
('AskWomenOver30', '2017-04-24 10:53:00', 'MoeBetterBooze'),
('EarthScience', '2012-01-27 05:22:00', 'scientologist2'),
('EarthScience', '2012-10-18 11:27:00', 'scientologist2'),
('EarthScience', '2013-02-21 04:10:00', 'scientologist2'),
('EarthScience', '2013-09-01 11:35:00', 'scientologist2'),
('EarthScience', '2013-12-06 09:22:00', 'scientologist2'),
('EarthScience', '2014-04-22 23:41:00', 'scientologist2'),
('EarthScience', '2015-07-03 05:43:00', 'dbcalo'),
('EarthScience', '2015-07-03 05:43:00', 'ugtug'),
('EarthScience', '2015-07-03 05:43:00', 'walrusparadise'),
('EarthScience', '2016-05-22 14:14:00', 'dbcalo'),
('EarthScience', '2016-05-22 14:14:00', 'ugtug'),
('EarthScience', '2016-05-22 14:14:00', 'walrusparadise'),
('EarthScience', '2016-05-22 14:14:00', 'GodRaine'),
('EarthScience', '2016-05-22 14:14:00', 'southernrock6'),
('EarthScience', '2016-09-21 21:33:00', 'dbcalo'),
('EarthScience', '2016-09-21 21:33:00', 'ugtug'),
('EarthScience', '2016-09-21 21:33:00', 'walrusparadise'),
('EarthScience', '2016-09-21 21:33:00', 'GodRaine'),
('EarthScience', '2016-09-21 21:33:00', 'southernrock6'),
('EarthScience', '2016-11-15 10:07:00', 'dbcalo'),
('EarthScience', '2016-11-15 10:07:00', 'ugtug'),
('EarthScience', '2016-11-15 10:07:00', 'walrusparadise'),
('EarthScience', '2016-11-15 10:07:00', 'GodRaine'),
('EarthScience', '2016-11-15 10:07:00', 'southernrock6'),
('EarthScience', '2017-03-01 18:14:00', 'dbcalo'),
('EarthScience', '2017-03-01 18:14:00', 'ugtug'),
('EarthScience', '2017-03-01 18:14:00', 'walrusparadise'),
('EarthScience', '2017-03-01 18:14:00', 'southernrock6'),
('EarthScience', '2018-01-29 08:47:00', 'dbcalo'),
('EarthScience', '2018-01-29 08:47:00', 'ugtug'),
('EarthScience', '2018-01-29 08:47:00', 'walrusparadise'),
('EarthScience', '2018-01-29 08:47:00', 'southernrock6'),
('EarthScience', '2018-01-29 08:47:00', 'Halcyon3k'),
('Eskrima', '2012-02-15 07:38:00', 'aw4lly'),
('Eskrima', '2013-08-31 03:46:00', 'aw4lly'),
('Eskrima', '2014-09-24 04:29:00', 'aw4lly'),
('Fzero', '2012-01-05 01:19:00', 'spirit-fox'),
('Fzero', '2012-01-05 01:19:00', 'fortune_cell'),
('Fzero', '2012-04-02 11:40:00', 'spirit-fox'),
('Fzero', '2012-04-02 11:40:00', 'fortune_cell'),
('Fzero', '2012-06-15 12:32:00', 'spirit-fox'),
('Fzero', '2012-06-15 12:32:00', 'fortune_cell'),
('Fzero', '2012-08-18 23:01:00', 'spirit-fox'),
('Fzero', '2012-08-18 23:01:00', 'fortune_cell'),
('Fzero', '2012-10-26 17:38:00', 'spirit-fox'),
('Fzero', '2012-10-26 17:38:00', 'fortune_cell'),
('Fzero', '2012-11-04 06:08:00', 'spirit-fox'),
('Fzero', '2012-11-04 06:08:00', 'fortune_cell'),
('Fzero', '2012-12-15 05:32:00', 'spirit-fox'),
('Fzero', '2012-12-15 05:32:00', 'fortune_cell'),
('Fzero', '2013-03-05 04:08:00', 'spirit-fox'),
('Fzero', '2013-03-05 04:08:00', 'fortune_cell'),
('Fzero', '2013-05-08 14:02:00', 'spirit-fox'),
('Fzero', '2013-05-08 14:02:00', 'fortune_cell'),
('Fzero', '2013-06-23 07:12:00', 'spirit-fox'),
('Fzero', '2013-06-23 07:12:00', 'fortune_cell'),
('Fzero', '2013-08-26 20:38:00', 'spirit-fox'),
('Fzero', '2013-08-26 20:38:00', 'fortune_cell'),
('Fzero', '2013-10-30 13:23:00', 'spirit-fox'),
('Fzero', '2013-10-30 13:23:00', 'fortune_cell'),
('Fzero', '2014-07-15 15:21:00', 'spirit-fox'),
('Fzero', '2014-07-15 15:21:00', 'fortune_cell'),
('Fzero', '2014-10-05 12:13:00', 'spirit-fox'),
('Fzero', '2014-10-05 12:13:00', 'fortune_cell'),
('Fzero', '2014-10-05 12:13:00', 'samueldlockhart'),
('Fzero', '2014-11-09 20:22:00', 'spirit-fox'),
('Fzero', '2014-11-09 20:22:00', 'fortune_cell'),
('Fzero', '2014-11-09 20:22:00', 'samueldlockhart'),
('Fzero', '2015-01-20 10:06:00', 'spirit-fox'),
('Fzero', '2015-01-20 10:06:00', 'fortune_cell'),
('Fzero', '2015-01-20 10:06:00', 'samueldlockhart'),
('Fzero', '2015-02-22 09:40:00', 'spirit-fox'),
('Fzero', '2015-02-22 09:40:00', 'fortune_cell'),
('Fzero', '2015-02-22 09:40:00', 'samueldlockhart'),
('Fzero', '2015-04-26 04:21:00', 'spirit-fox'),
('Fzero', '2015-04-26 04:21:00', 'fortune_cell'),
('Fzero', '2015-04-26 04:21:00', 'samueldlockhart'),
('Fzero', '2015-05-30 08:05:00', 'spirit-fox'),
('Fzero', '2015-05-30 08:05:00', 'fortune_cell'),
('Fzero', '2015-05-30 08:05:00', 'fart-princess'),
('Fzero', '2015-10-19 18:21:00', 'spirit-fox'),
('Fzero', '2015-10-19 18:21:00', 'fortune_cell'),
('Fzero', '2015-10-19 18:21:00', 'fart-princess'),
('Fzero', '2016-02-26 17:07:00', 'spirit-fox'),
('Fzero', '2016-02-26 17:07:00', 'fortune_cell'),
('Fzero', '2016-02-26 17:07:00', 'fart-princess'),
('Fzero', '2016-04-02 07:48:00', 'spirit-fox'),
('Fzero', '2016-04-02 07:48:00', 'fortune_cell'),
('Fzero', '2016-04-02 07:48:00', 'fart-princess'),
('Fzero', '2016-09-01 07:19:00', 'spirit-fox'),
('Fzero', '2016-09-01 07:19:00', 'fortune_cell'),
('Fzero', '2016-09-01 07:19:00', 'fart-princess'),
('Fzero', '2017-03-30 08:32:00', 'spirit-fox'),
('Fzero', '2017-03-30 08:32:00', 'fortune_cell'),
('Fzero', '2017-03-30 08:32:00', 'fart-princess'),
('Fzero', '2017-03-30 08:32:00', 'Electropolitan'),
('Fzero', '2017-03-30 08:32:00', 'Rmac524'),
('Fzero', '2017-12-07 18:39:00', 'spirit-fox'),
('Fzero', '2017-12-07 18:39:00', 'fortune_cell'),
('Fzero', '2017-12-07 18:39:00', 'fart-princess'),
('Fzero', '2017-12-07 18:39:00', 'Electropolitan'),
('Fzero', '2017-12-07 18:39:00', 'Rmac524'),
('Fzero', '2017-12-07 18:39:00', 'akc12'),
('Fzero', '2017-12-07 18:39:00', 'F-ZEROCentral'),
('Fzero', '2017-12-07 18:39:00', 'Wanderrer21'),
('Fzero', '2018-02-21 10:27:00', 'spirit-fox'),
('Fzero', '2018-02-21 10:27:00', 'fortune_cell'),
('Fzero', '2018-02-21 10:27:00', 'fart-princess'),
('Fzero', '2018-02-21 10:27:00', 'Electropolitan'),
('Fzero', '2018-02-21 10:27:00', 'Rmac524'),
('Fzero', '2018-02-21 10:27:00', 'akc12'),
('Fzero', '2018-02-21 10:27:00', 'F-ZEROCentral'),
('Fzero', '2018-02-21 10:27:00', 'Wanderrer21'),
('Fzero', '2018-03-05 17:26:00', 'spirit-fox'),
('Fzero', '2018-03-05 17:26:00', 'fortune_cell'),
('Fzero', '2018-03-05 17:26:00', 'fart-princess'),
('Fzero', '2018-03-05 17:26:00', 'Electropolitan'),
('Fzero', '2018-03-05 17:26:00', 'Rmac524'),
('Fzero', '2018-03-05 17:26:00', 'akc12'),
('Fzero', '2018-03-05 17:26:00', 'F-ZEROCentral'),
('Fzero', '2018-03-05 17:26:00', 'Wanderrer21'),
('Fzero', '2018-12-13 00:20:00', 'spirit-fox'),
('Fzero', '2018-12-13 00:20:00', 'fortune_cell'),
('Fzero', '2018-12-13 00:20:00', 'fart-princess'),
('Fzero', '2018-12-13 00:20:00', 'Electropolitan'),
('Fzero', '2018-12-13 00:20:00', 'Rmac524'),
('Fzero', '2018-12-13 00:20:00', 'akc12'),
('Fzero', '2018-12-13 00:20:00', 'F-ZEROCentral'),
('Fzero', '2018-12-13 00:20:00', 'Wanderrer21'),
('Fzero', '2018-12-13 00:20:00', 'Siontix'),
('Fzero', '2018-12-13 00:20:00', 'Figh16'),
('LinusTechTips', '2016-07-31 20:12:00', 'Frosstic'),
('LinusTechTips', '2016-07-31 20:12:00', 'ImFrazle'),
('LinusTechTips', '2017-04-01 20:20:00', 'Frosstic'),
('LinusTechTips', '2017-04-01 20:20:00', 'ImFrazle'),
('LinusTechTips', '2017-04-01 20:20:00', 'Caltane'),
('LinusTechTips', '2017-09-10 17:34:00', 'Frosstic'),
('LinusTechTips', '2017-09-10 17:34:00', 'ImFrazle'),
('LinusTechTips', '2017-09-10 17:34:00', 'Caltane'),
('LinusTechTips', '2017-11-16 14:45:00', 'Frosstic'),
('LinusTechTips', '2017-11-16 14:45:00', 'ImFrazle'),
('LinusTechTips', '2017-11-16 14:45:00', 'Caltane'),
('LinusTechTips', '2017-12-12 12:25:00', 'Frosstic'),
('LinusTechTips', '2017-12-12 12:25:00', 'ImFrazle'),
('LinusTechTips', '2017-12-12 12:25:00', 'Caltane'),
('LinusTechTips', '2018-03-02 20:23:00', 'Frosstic'),
('LinusTechTips', '2018-03-02 20:23:00', 'ImFrazle'),
('LinusTechTips', '2018-03-02 20:23:00', 'Caltane'),
('LinusTechTips', '2019-02-20 14:37:00', 'Frosstic'),
('LinusTechTips', '2019-02-20 14:37:00', 'ImFrazle'),
('LinusTechTips', '2019-02-20 14:37:00', 'Caltane'),
('LinusTechTips', '2019-02-20 14:37:00', 'LMG_Aprime'),
('LinusTechTips', '2019-02-20 14:37:00', 'NickLTT'),
('LinusTechTips', '2019-02-20 14:37:00', 'AnthonyLTT'),
('UpliftingNews', '2012-05-19 20:26:00', 'razorsheldon'),
('UpliftingNews', '2012-06-02 08:51:00', 'razorsheldon'),
('UpliftingNews', '2012-07-12 12:20:00', 'razorsheldon'),
('UpliftingNews', '2012-08-02 03:39:00', 'razorsheldon'),
('UpliftingNews', '2012-09-11 11:10:00', 'razorsheldon'),
('UpliftingNews', '2012-10-04 19:41:00', 'razorsheldon'),
('UpliftingNews', '2012-10-04 19:41:00', 'Loate'),
('UpliftingNews', '2012-10-04 19:41:00', 'zachinoz'),
('UpliftingNews', '2012-10-04 19:41:00', 'Tubemonster'),
('UpliftingNews', '2012-11-06 01:03:00', 'razorsheldon'),
('UpliftingNews', '2012-11-06 01:03:00', 'Loate'),
('UpliftingNews', '2012-11-06 01:03:00', 'zachinoz'),
('UpliftingNews', '2012-11-06 01:03:00', 'Tubemonster'),
('UpliftingNews', '2012-12-01 00:15:00', 'razorsheldon'),
('UpliftingNews', '2012-12-01 00:15:00', 'Loate'),
('UpliftingNews', '2012-12-01 00:15:00', 'zachinoz'),
('UpliftingNews', '2012-12-01 00:15:00', 'Tubemonster'),
('UpliftingNews', '2012-12-01 00:15:00', 'AutoModerator'),
('UpliftingNews', '2013-01-02 12:39:00', 'razorsheldon'),
('UpliftingNews', '2013-01-02 12:39:00', 'Loate'),
('UpliftingNews', '2013-01-02 12:39:00', 'zachinoz'),
('UpliftingNews', '2013-01-02 12:39:00', 'Tubemonster'),
('UpliftingNews', '2013-01-02 12:39:00', 'AutoModerator'),
('UpliftingNews', '2013-02-05 17:10:00', 'razorsheldon'),
('UpliftingNews', '2013-02-05 17:10:00', 'Loate'),
('UpliftingNews', '2013-02-05 17:10:00', 'zachinoz'),
('UpliftingNews', '2013-02-05 17:10:00', 'Tubemonster'),
('UpliftingNews', '2013-02-05 17:10:00', 'AutoModerator'),
('UpliftingNews', '2013-02-05 17:10:00', 'UpliftingNews'),
('UpliftingNews', '2013-03-12 12:47:00', 'razorsheldon'),
('UpliftingNews', '2013-03-12 12:47:00', 'Loate'),
('UpliftingNews', '2013-03-12 12:47:00', 'zachinoz'),
('UpliftingNews', '2013-03-12 12:47:00', 'Tubemonster'),
('UpliftingNews', '2013-03-12 12:47:00', 'AutoModerator'),
('UpliftingNews', '2013-03-12 12:47:00', 'UpliftingNews'),
('UpliftingNews', '2013-04-02 22:41:00', 'razorsheldon'),
('UpliftingNews', '2013-04-02 22:41:00', 'AutoModerator'),
('UpliftingNews', '2013-05-01 12:55:00', 'razorsheldon'),
('UpliftingNews', '2013-05-01 12:55:00', 'AutoModerator'),
('UpliftingNews', '2013-06-01 13:03:00', 'razorsheldon'),
('UpliftingNews', '2013-06-01 13:03:00', 'AutoModerator'),
('UpliftingNews', '2013-07-01 12:58:00', 'razorsheldon'),
('UpliftingNews', '2013-07-01 12:58:00', 'AutoModerator'),
('UpliftingNews', '2013-07-01 12:58:00', 'UpliftingNews'),
('UpliftingNews', '2013-07-01 12:58:00', 'creativezen'),
('UpliftingNews', '2013-08-01 13:05:00', 'razorsheldon'),
('UpliftingNews', '2013-08-01 13:05:00', 'AutoModerator'),
('UpliftingNews', '2013-08-01 13:05:00', 'UpliftingNews'),
('UpliftingNews', '2013-08-01 13:05:00', 'creativezen'),
('UpliftingNews', '2013-09-01 13:00:00', 'razorsheldon'),
('UpliftingNews', '2013-09-01 13:00:00', 'AutoModerator'),
('UpliftingNews', '2013-09-01 13:00:00', 'UpliftingNews'),
('UpliftingNews', '2013-09-01 13:00:00', 'creativezen'),
('UpliftingNews', '2013-09-01 13:00:00', 'amputeenager'),
('UpliftingNews', '2013-10-01 12:59:00', 'razorsheldon'),
('UpliftingNews', '2013-10-01 12:59:00', 'AutoModerator'),
('UpliftingNews', '2013-10-01 12:59:00', 'UpliftingNews'),
('UpliftingNews', '2013-10-01 12:59:00', 'amputeenager'),
('UpliftingNews', '2013-10-01 12:59:00', 'NapoleonBonerparts'),
('UpliftingNews', '2013-11-01 03:43:00', 'razorsheldon'),
('UpliftingNews', '2013-11-01 03:43:00', 'AutoModerator'),
('UpliftingNews', '2013-11-01 03:43:00', 'UpliftingNews'),
('UpliftingNews', '2013-11-01 03:43:00', 'amputeenager'),
('UpliftingNews', '2013-11-01 03:43:00', 'NapoleonBonerparts'),
('UpliftingNews', '2013-12-01 13:39:00', 'razorsheldon'),
('UpliftingNews', '2013-12-01 13:39:00', 'AutoModerator'),
('UpliftingNews', '2013-12-01 13:39:00', 'UpliftingNews'),
('UpliftingNews', '2013-12-01 13:39:00', 'amputeenager'),
('UpliftingNews', '2013-12-01 13:39:00', 'NapoleonBonerparts'),
('UpliftingNews', '2014-01-01 13:57:00', 'razorsheldon'),
('UpliftingNews', '2014-01-01 13:57:00', 'AutoModerator'),
('UpliftingNews', '2014-01-01 13:57:00', 'UpliftingNews'),
('UpliftingNews', '2014-01-01 13:57:00', 'amputeenager'),
('UpliftingNews', '2014-01-01 13:57:00', 'NapoleonBonerparts'),
('UpliftingNews', '2014-02-01 14:40:00', 'razorsheldon'),
('UpliftingNews', '2014-02-01 14:40:00', 'AutoModerator'),
('UpliftingNews', '2014-02-01 14:40:00', 'UpliftingNews'),
('UpliftingNews', '2014-02-01 14:40:00', 'amputeenager'),
('UpliftingNews', '2014-02-01 14:40:00', 'NapoleonBonerparts'),
('UpliftingNews', '2014-03-01 03:42:00', 'razorsheldon'),
('UpliftingNews', '2014-03-01 03:42:00', 'AutoModerator'),
('UpliftingNews', '2014-03-01 03:42:00', 'UpliftingNews'),
('UpliftingNews', '2014-03-01 03:42:00', 'amputeenager'),
('UpliftingNews', '2014-03-01 03:42:00', 'NapoleonBonerparts'),
('UpliftingNews', '2014-03-01 03:42:00', 'Paradox'),
('UpliftingNews', '2014-04-06 04:47:00', 'razorsheldon'),
('UpliftingNews', '2014-04-06 04:47:00', 'AutoModerator'),
('UpliftingNews', '2014-04-06 04:47:00', 'UpliftingNews'),
('UpliftingNews', '2014-04-06 04:47:00', 'amputeenager'),
('UpliftingNews', '2014-04-06 04:47:00', 'NapoleonBonerparts'),
('UpliftingNews', '2014-04-06 04:47:00', 'Paradox'),
('UpliftingNews', '2014-05-04 06:24:00', 'razorsheldon'),
('UpliftingNews', '2014-05-04 06:24:00', 'AutoModerator'),
('UpliftingNews', '2014-05-04 06:24:00', 'UpliftingNews'),
('UpliftingNews', '2014-05-04 06:24:00', 'amputeenager'),
('UpliftingNews', '2014-05-04 06:24:00', 'NapoleonBonerparts'),
('UpliftingNews', '2014-05-04 06:24:00', 'Paradox'),
('UpliftingNews', '2014-06-01 07:34:00', 'razorsheldon'),
('UpliftingNews', '2014-06-01 07:34:00', 'AutoModerator'),
('UpliftingNews', '2014-06-01 07:34:00', 'UpliftingNews'),
('UpliftingNews', '2014-06-01 07:34:00', 'amputeenager'),
('UpliftingNews', '2014-07-03 05:19:00', 'razorsheldon'),
('UpliftingNews', '2014-07-03 05:19:00', 'AutoModerator'),
('UpliftingNews', '2014-07-03 05:19:00', 'UpliftingNews'),
('UpliftingNews', '2014-07-03 05:19:00', 'amputeenager'),
('UpliftingNews', '2014-07-03 05:19:00', 'letmewritethatdown'),
('UpliftingNews', '2014-08-01 07:59:00', 'razorsheldon'),
('UpliftingNews', '2014-08-01 07:59:00', 'AutoModerator'),
('UpliftingNews', '2014-08-01 07:59:00', 'UpliftingNews'),
('UpliftingNews', '2014-08-01 07:59:00', 'amputeenager'),
('UpliftingNews', '2014-08-01 07:59:00', 'letmewritethatdown'),
('UpliftingNews', '2014-09-03 18:48:00', 'razorsheldon'),
('UpliftingNews', '2014-09-03 18:48:00', 'AutoModerator'),
('UpliftingNews', '2014-09-03 18:48:00', 'UpliftingNews'),
('UpliftingNews', '2014-09-03 18:48:00', 'amputeenager'),
('UpliftingNews', '2014-09-03 18:48:00', 'letmewritethatdown'),
('UpliftingNews', '2014-10-05 11:25:00', 'razorsheldon'),
('UpliftingNews', '2014-10-05 11:25:00', 'AutoModerator'),
('UpliftingNews', '2014-10-05 11:25:00', 'UpliftingNews'),
('UpliftingNews', '2014-10-05 11:25:00', 'amputeenager'),
('UpliftingNews', '2014-10-05 11:25:00', 'letmewritethatdown'),
('UpliftingNews', '2014-11-04 23:12:00', 'razorsheldon'),
('UpliftingNews', '2014-11-04 23:12:00', 'AutoModerator'),
('UpliftingNews', '2014-11-04 23:12:00', 'UpliftingNews'),
('UpliftingNews', '2014-11-04 23:12:00', 'amputeenager'),
('UpliftingNews', '2014-11-04 23:12:00', 'letmewritethatdown'),
('UpliftingNews', '2014-12-02 02:49:00', 'razorsheldon'),
('UpliftingNews', '2014-12-02 02:49:00', 'AutoModerator'),
('UpliftingNews', '2014-12-02 02:49:00', 'UpliftingNews'),
('UpliftingNews', '2014-12-02 02:49:00', 'amputeenager'),
('UpliftingNews', '2014-12-02 02:49:00', 'letmewritethatdown'),
('UpliftingNews', '2015-01-06 05:56:00', 'razorsheldon'),
('UpliftingNews', '2015-01-06 05:56:00', 'AutoModerator'),
('UpliftingNews', '2015-01-06 05:56:00', 'UpliftingNews'),
('UpliftingNews', '2015-01-06 05:56:00', 'amputeenager'),
('UpliftingNews', '2015-01-06 05:56:00', 'letmewritethatdown'),
('UpliftingNews', '2015-01-06 05:56:00', 'redhotkurt'),
('UpliftingNews', '2015-02-01 16:00:00', 'razorsheldon'),
('UpliftingNews', '2015-02-01 16:00:00', 'AutoModerator'),
('UpliftingNews', '2015-02-01 16:00:00', 'UpliftingNews'),
('UpliftingNews', '2015-02-01 16:00:00', 'amputeenager'),
('UpliftingNews', '2015-02-01 16:00:00', 'letmewritethatdown'),
('UpliftingNews', '2015-02-01 16:00:00', 'redhotkurt'),
('UpliftingNews', '2015-03-09 21:54:00', 'razorsheldon'),
('UpliftingNews', '2015-03-09 21:54:00', 'AutoModerator'),
('UpliftingNews', '2015-03-09 21:54:00', 'UpliftingNews'),
('UpliftingNews', '2015-03-09 21:54:00', 'amputeenager'),
('UpliftingNews', '2015-03-09 21:54:00', 'redhotkurt'),
('UpliftingNews', '2015-03-09 21:54:00', 'Dodye'),
('UpliftingNews', '2015-04-07 01:12:00', 'razorsheldon'),
('UpliftingNews', '2015-04-07 01:12:00', 'AutoModerator'),
('UpliftingNews', '2015-04-07 01:12:00', 'UpliftingNews'),
('UpliftingNews', '2015-04-07 01:12:00', 'amputeenager'),
('UpliftingNews', '2015-04-07 01:12:00', 'redhotkurt'),
('UpliftingNews', '2015-04-07 01:12:00', 'Dodye'),
('UpliftingNews', '2015-05-02 02:23:00', 'razorsheldon'),
('UpliftingNews', '2015-05-02 02:23:00', 'AutoModerator'),
('UpliftingNews', '2015-05-02 02:23:00', 'UpliftingNews'),
('UpliftingNews', '2015-05-02 02:23:00', 'amputeenager'),
('UpliftingNews', '2015-05-02 02:23:00', 'redhotkurt'),
('UpliftingNews', '2015-05-02 02:23:00', 'Dodye'),
('UpliftingNews', '2015-05-02 02:23:00', 'invalid_username-'),
('UpliftingNews', '2015-06-02 21:15:00', 'razorsheldon'),
('UpliftingNews', '2015-06-02 21:15:00', 'UpliftingNews'),
('UpliftingNews', '2015-06-02 21:15:00', 'amputeenager'),
('UpliftingNews', '2015-06-02 21:15:00', 'Dodye'),
('UpliftingNews', '2015-06-02 21:15:00', 'invalid_username-'),
('UpliftingNews', '2015-06-02 21:15:00', 'love_the_heat'),
('UpliftingNews', '2015-07-03 02:00:00', 'razorsheldon'),
('UpliftingNews', '2015-07-03 02:00:00', 'UpliftingNews'),
('UpliftingNews', '2015-07-03 02:00:00', 'amputeenager'),
('UpliftingNews', '2015-07-03 02:00:00', 'Dodye'),
('UpliftingNews', '2015-07-03 02:00:00', 'invalid_username-'),
('UpliftingNews', '2015-07-03 02:00:00', 'love_the_heat'),
('UpliftingNews', '2015-08-12 08:57:00', 'razorsheldon'),
('UpliftingNews', '2015-08-12 08:57:00', 'UpliftingNews'),
('UpliftingNews', '2015-08-12 08:57:00', 'amputeenager'),
('UpliftingNews', '2015-08-12 08:57:00', 'invalid_username-'),
('UpliftingNews', '2015-08-12 08:57:00', 'love_the_heat'),
('UpliftingNews', '2015-10-15 19:56:00', 'razorsheldon'),
('UpliftingNews', '2015-10-15 19:56:00', 'UpliftingNews'),
('UpliftingNews', '2015-10-15 19:56:00', 'amputeenager'),
('UpliftingNews', '2015-10-15 19:56:00', 'invalid_username-'),
('UpliftingNews', '2015-10-15 19:56:00', 'love_the_heat'),
('UpliftingNews', '2015-10-15 19:56:00', 'Greypo'),
('UpliftingNews', '2015-11-13 01:32:00', 'razorsheldon'),
('UpliftingNews', '2015-11-13 01:32:00', 'UpliftingNews'),
('UpliftingNews', '2015-11-13 01:32:00', 'amputeenager'),
('UpliftingNews', '2015-11-13 01:32:00', 'invalid_username-'),
('UpliftingNews', '2015-11-13 01:32:00', 'love_the_heat'),
('UpliftingNews', '2015-11-13 01:32:00', 'Greypo'),
('UpliftingNews', '2015-12-04 03:44:00', 'razorsheldon'),
('UpliftingNews', '2015-12-04 03:44:00', 'UpliftingNews'),
('UpliftingNews', '2015-12-04 03:44:00', 'amputeenager'),
('UpliftingNews', '2015-12-04 03:44:00', 'love_the_heat'),
('UpliftingNews', '2015-12-04 03:44:00', 'Greypo'),
('UpliftingNews', '2016-01-01 06:09:00', 'razorsheldon'),
('UpliftingNews', '2016-01-01 06:09:00', 'UpliftingNews'),
('UpliftingNews', '2016-01-01 06:09:00', 'amputeenager'),
('UpliftingNews', '2016-01-01 06:09:00', 'love_the_heat'),
('UpliftingNews', '2016-01-01 06:09:00', 'Greypo'),
('UpliftingNews', '2016-02-01 22:15:00', 'razorsheldon'),
('UpliftingNews', '2016-02-01 22:15:00', 'UpliftingNews'),
('UpliftingNews', '2016-02-01 22:15:00', 'amputeenager'),
('UpliftingNews', '2016-02-01 22:15:00', 'love_the_heat'),
('UpliftingNews', '2016-02-01 22:15:00', 'Greypo'),
('UpliftingNews', '2016-03-01 00:44:00', 'razorsheldon'),
('UpliftingNews', '2016-03-01 00:44:00', 'UpliftingNews'),
('UpliftingNews', '2016-03-01 00:44:00', 'amputeenager'),
('UpliftingNews', '2016-03-01 00:44:00', 'love_the_heat'),
('UpliftingNews', '2016-03-01 00:44:00', 'Greypo'),
('UpliftingNews', '2016-03-01 00:44:00', 'siouxsie_siouxv2'),
('UpliftingNews', '2016-03-01 00:44:00', 'MeghanAM'),
('UpliftingNews', '2016-03-01 00:44:00', 'JoyousCacophony'),
('UpliftingNews', '2016-04-01 09:18:00', 'razorsheldon'),
('UpliftingNews', '2016-04-01 09:18:00', 'UpliftingNews'),
('UpliftingNews', '2016-04-01 09:18:00', 'amputeenager'),
('UpliftingNews', '2016-04-01 09:18:00', 'love_the_heat'),
('UpliftingNews', '2016-04-01 09:18:00', 'siouxsie_siouxv2'),
('UpliftingNews', '2016-04-01 09:18:00', 'MeghanAM'),
('UpliftingNews', '2016-05-04 01:17:00', 'razorsheldon'),
('UpliftingNews', '2016-05-04 01:17:00', 'UpliftingNews'),
('UpliftingNews', '2016-05-04 01:17:00', 'amputeenager'),
('UpliftingNews', '2016-05-04 01:17:00', 'love_the_heat'),
('UpliftingNews', '2016-05-04 01:17:00', 'siouxsie_siouxv2'),
('UpliftingNews', '2016-05-04 01:17:00', 'MeghanAM'),
('UpliftingNews', '2016-06-02 02:51:00', 'razorsheldon'),
('UpliftingNews', '2016-06-02 02:51:00', 'UpliftingNews'),
('UpliftingNews', '2016-06-02 02:51:00', 'amputeenager'),
('UpliftingNews', '2016-06-02 02:51:00', 'love_the_heat'),
('UpliftingNews', '2016-06-02 02:51:00', 'siouxsie_siouxv2'),
('UpliftingNews', '2016-06-02 02:51:00', 'MeghanAM'),
('UpliftingNews', '2016-07-02 19:08:00', 'razorsheldon'),
('UpliftingNews', '2016-07-02 19:08:00', 'UpliftingNews'),
('UpliftingNews', '2016-07-02 19:08:00', 'amputeenager'),
('UpliftingNews', '2016-07-02 19:08:00', 'love_the_heat'),
('UpliftingNews', '2016-07-02 19:08:00', 'siouxsie_siouxv2'),
('UpliftingNews', '2016-07-02 19:08:00', 'MeghanAM'),
('UpliftingNews', '2016-08-01 01:54:00', 'razorsheldon'),
('UpliftingNews', '2016-08-01 01:54:00', 'UpliftingNews'),
('UpliftingNews', '2016-08-01 01:54:00', 'amputeenager'),
('UpliftingNews', '2016-08-01 01:54:00', 'love_the_heat'),
('UpliftingNews', '2016-08-01 01:54:00', 'siouxsie_siouxv2'),
('UpliftingNews', '2016-08-01 01:54:00', 'MeghanAM'),
('UpliftingNews', '2016-08-01 01:54:00', 'Hilltopchill'),
('UpliftingNews', '2016-08-01 01:54:00', 'maybesaydie'),
('UpliftingNews', '2016-08-01 01:54:00', 'DavidLuizshair'),
('UpliftingNews', '2016-09-01 05:32:00', 'razorsheldon'),
('UpliftingNews', '2016-09-01 05:32:00', 'UpliftingNews'),
('UpliftingNews', '2016-09-01 05:32:00', 'amputeenager'),
('UpliftingNews', '2016-09-01 05:32:00', 'love_the_heat'),
('UpliftingNews', '2016-09-01 05:32:00', 'siouxsie_siouxv2'),
('UpliftingNews', '2016-09-01 05:32:00', 'MeghanAM'),
('UpliftingNews', '2016-09-01 05:32:00', 'Hilltopchill'),
('UpliftingNews', '2016-09-01 05:32:00', 'maybesaydie'),
('UpliftingNews', '2016-09-01 05:32:00', 'DavidLuizshair'),
('UpliftingNews', '2016-10-01 01:04:00', 'razorsheldon'),
('UpliftingNews', '2016-10-01 01:04:00', 'UpliftingNews'),
('UpliftingNews', '2016-10-01 01:04:00', 'amputeenager'),
('UpliftingNews', '2016-10-01 01:04:00', 'siouxsie_siouxv2'),
('UpliftingNews', '2016-10-01 01:04:00', 'Hilltopchill'),
('UpliftingNews', '2016-10-01 01:04:00', 'DavidLuizshair'),
('UpliftingNews', '2016-11-03 00:05:00', 'razorsheldon'),
('UpliftingNews', '2016-11-03 00:05:00', 'UpliftingNews'),
('UpliftingNews', '2016-11-03 00:05:00', 'amputeenager'),
('UpliftingNews', '2016-11-03 00:05:00', 'StanGibson18'),
('UpliftingNews', '2016-12-01 21:09:00', 'razorsheldon'),
('UpliftingNews', '2016-12-01 21:09:00', 'UpliftingNews'),
('UpliftingNews', '2016-12-01 21:09:00', 'amputeenager'),
('UpliftingNews', '2016-12-01 21:09:00', 'StanGibson18'),
('UpliftingNews', '2017-01-01 12:45:00', 'razorsheldon'),
('UpliftingNews', '2017-01-01 12:45:00', 'UpliftingNews'),
('UpliftingNews', '2017-01-01 12:45:00', 'amputeenager'),
('UpliftingNews', '2017-01-01 12:45:00', 'StanGibson18'),
('UpliftingNews', '2017-01-01 12:45:00', 'iBleeedorange'),
('UpliftingNews', '2017-02-02 08:26:00', 'razorsheldon'),
('UpliftingNews', '2017-02-02 08:26:00', 'UpliftingNews'),
('UpliftingNews', '2017-02-02 08:26:00', 'amputeenager'),
('UpliftingNews', '2017-02-02 08:26:00', 'StanGibson18'),
('UpliftingNews', '2017-02-02 08:26:00', 'iBleeedorange'),
('UpliftingNews', '2017-03-02 18:50:00', 'razorsheldon'),
('UpliftingNews', '2017-03-02 18:50:00', 'UpliftingNews'),
('UpliftingNews', '2017-03-02 18:50:00', 'amputeenager'),
('UpliftingNews', '2017-03-02 18:50:00', 'StanGibson18'),
('UpliftingNews', '2017-03-02 18:50:00', 'iBleeedorange'),
('UpliftingNews', '2017-03-02 18:50:00', 'labmonkey01'),
('UpliftingNews', '2017-04-01 11:10:00', 'razorsheldon'),
('UpliftingNews', '2017-04-01 11:10:00', 'UpliftingNews'),
('UpliftingNews', '2017-04-01 11:10:00', 'amputeenager'),
('UpliftingNews', '2017-04-01 11:10:00', 'StanGibson18'),
('UpliftingNews', '2017-04-01 11:10:00', 'iBleeedorange'),
('UpliftingNews', '2017-04-01 11:10:00', 'labmonkey01'),
('UpliftingNews', '2017-05-02 00:09:00', 'razorsheldon'),
('UpliftingNews', '2017-05-02 00:09:00', 'UpliftingNews'),
('UpliftingNews', '2017-05-02 00:09:00', 'amputeenager'),
('UpliftingNews', '2017-05-02 00:09:00', 'StanGibson18'),
('UpliftingNews', '2017-05-02 00:09:00', 'iBleeedorange'),
('UpliftingNews', '2017-05-02 00:09:00', 'labmonkey01'),
('UpliftingNews', '2017-06-01 01:28:00', 'razorsheldon'),
('UpliftingNews', '2017-06-01 01:28:00', 'UpliftingNews'),
('UpliftingNews', '2017-06-01 01:28:00', 'amputeenager'),
('UpliftingNews', '2017-06-01 01:28:00', 'StanGibson18'),
('UpliftingNews', '2017-06-01 01:28:00', 'iBleeedorange'),
('UpliftingNews', '2017-06-01 01:28:00', 'labmonkey01'),
('UpliftingNews', '2017-07-01 04:22:00', 'razorsheldon'),
('UpliftingNews', '2017-07-01 04:22:00', 'UpliftingNews'),
('UpliftingNews', '2017-07-01 04:22:00', 'amputeenager'),
('UpliftingNews', '2017-07-01 04:22:00', 'StanGibson18'),
('UpliftingNews', '2017-07-01 04:22:00', 'iBleeedorange'),
('UpliftingNews', '2017-07-01 04:22:00', 'labmonkey01'),
('UpliftingNews', '2017-08-01 14:55:00', 'razorsheldon'),
('UpliftingNews', '2017-08-01 14:55:00', 'UpliftingNews'),
('UpliftingNews', '2017-08-01 14:55:00', 'amputeenager'),
('UpliftingNews', '2017-08-01 14:55:00', 'StanGibson18'),
('UpliftingNews', '2017-08-01 14:55:00', 'iBleeedorange'),
('UpliftingNews', '2017-08-01 14:55:00', 'labmonkey01'),
('UpliftingNews', '2017-09-01 08:55:00', 'razorsheldon'),
('UpliftingNews', '2017-09-01 08:55:00', 'UpliftingNews'),
('UpliftingNews', '2017-09-01 08:55:00', 'amputeenager'),
('UpliftingNews', '2017-09-01 08:55:00', 'StanGibson18'),
('UpliftingNews', '2017-09-01 08:55:00', 'iBleeedorange'),
('UpliftingNews', '2017-09-01 08:55:00', 'labmonkey01'),
('UpliftingNews', '2017-10-01 13:54:00', 'razorsheldon'),
('UpliftingNews', '2017-10-01 13:54:00', 'UpliftingNews'),
('UpliftingNews', '2017-10-01 13:54:00', 'amputeenager'),
('UpliftingNews', '2017-10-01 13:54:00', 'StanGibson18'),
('UpliftingNews', '2017-10-01 13:54:00', 'iBleeedorange'),
('UpliftingNews', '2017-10-01 13:54:00', 'labmonkey01'),
('UpliftingNews', '2017-11-01 13:24:00', 'razorsheldon'),
('UpliftingNews', '2017-11-01 13:24:00', 'UpliftingNews'),
('UpliftingNews', '2017-11-01 13:24:00', 'amputeenager'),
('UpliftingNews', '2017-11-01 13:24:00', 'StanGibson18'),
('UpliftingNews', '2017-11-01 13:24:00', 'iBleeedorange'),
('UpliftingNews', '2017-11-01 13:24:00', 'labmonkey01'),
('UpliftingNews', '2017-12-01 09:43:00', 'razorsheldon'),
('UpliftingNews', '2017-12-01 09:43:00', 'UpliftingNews'),
('UpliftingNews', '2017-12-01 09:43:00', 'amputeenager'),
('UpliftingNews', '2017-12-01 09:43:00', 'StanGibson18'),
('UpliftingNews', '2017-12-01 09:43:00', 'iBleeedorange'),
('UpliftingNews', '2017-12-01 09:43:00', 'labmonkey01'),
('UpliftingNews', '2017-12-01 09:43:00', 'DXGypsy'),
('UpliftingNews', '2018-01-01 15:22:00', 'razorsheldon'),
('UpliftingNews', '2018-01-01 15:22:00', 'UpliftingNews'),
('UpliftingNews', '2018-01-01 15:22:00', 'amputeenager'),
('UpliftingNews', '2018-01-01 15:22:00', 'StanGibson18'),
('UpliftingNews', '2018-01-01 15:22:00', 'iBleeedorange'),
('UpliftingNews', '2018-01-01 15:22:00', 'labmonkey01'),
('UpliftingNews', '2018-01-01 15:22:00', 'DXGypsy'),
('UpliftingNews', '2018-02-01 00:47:00', 'razorsheldon'),
('UpliftingNews', '2018-02-01 00:47:00', 'UpliftingNews'),
('UpliftingNews', '2018-02-01 00:47:00', 'amputeenager'),
('UpliftingNews', '2018-02-01 00:47:00', 'StanGibson18'),
('UpliftingNews', '2018-02-01 00:47:00', 'iBleeedorange'),
('UpliftingNews', '2018-02-01 00:47:00', 'labmonkey01'),
('UpliftingNews', '2018-02-01 00:47:00', 'DXGypsy'),
('UpliftingNews', '2018-03-01 04:35:00', 'razorsheldon'),
('UpliftingNews', '2018-03-01 04:35:00', 'UpliftingNews'),
('UpliftingNews', '2018-03-01 04:35:00', 'amputeenager'),
('UpliftingNews', '2018-03-01 04:35:00', 'StanGibson18'),
('UpliftingNews', '2018-03-01 04:35:00', 'iBleeedorange'),
('UpliftingNews', '2018-03-01 04:35:00', 'labmonkey01'),
('UpliftingNews', '2018-03-01 04:35:00', 'DXGypsy'),
('UpliftingNews', '2018-03-01 04:35:00', 'Kobobzane'),
('UpliftingNews', '2018-04-01 10:44:00', 'razorsheldon'),
('UpliftingNews', '2018-04-01 10:44:00', 'UpliftingNews'),
('UpliftingNews', '2018-04-01 10:44:00', 'amputeenager'),
('UpliftingNews', '2018-04-01 10:44:00', 'StanGibson18'),
('UpliftingNews', '2018-04-01 10:44:00', 'iBleeedorange'),
('UpliftingNews', '2018-04-01 10:44:00', 'labmonkey01'),
('UpliftingNews', '2018-04-01 10:44:00', 'DXGypsy'),
('UpliftingNews', '2018-04-01 10:44:00', 'Kobobzane'),
('UpliftingNews', '2019-03-01 00:32:00', 'razorsheldon'),
('UpliftingNews', '2019-03-01 00:32:00', 'UpliftingNews'),
('UpliftingNews', '2019-03-01 00:32:00', 'amputeenager'),
('UpliftingNews', '2019-03-01 00:32:00', 'StanGibson18'),
('UpliftingNews', '2019-03-01 00:32:00', 'iBleeedorange'),
('UpliftingNews', '2019-03-01 00:32:00', 'labmonkey01'),
('UpliftingNews', '2019-03-01 00:32:00', 'DXGypsy'),
('UpliftingNews', '2019-03-01 00:32:00', 'Kobobzane'),
('WomensSoccer', '2013-08-16 16:29:00', 'bendh18'),
('WomensSoccer', '2013-08-16 16:29:00', 'DylanNoPants'),
('WomensSoccer', '2013-08-16 16:29:00', 'Crusaruis28'),
('WomensSoccer', '2013-08-16 16:29:00', 'timeofyoursong'),
('WomensSoccer', '2014-02-03 13:19:00', 'bendh18'),
('WomensSoccer', '2014-02-03 13:19:00', 'DylanNoPants'),
('WomensSoccer', '2014-02-03 13:19:00', 'Crusaruis28'),
('WomensSoccer', '2014-02-03 13:19:00', 'timeofyoursong'),
('WomensSoccer', '2014-04-27 07:58:00', 'bendh18'),
('WomensSoccer', '2014-04-27 07:58:00', 'DylanNoPants'),
('WomensSoccer', '2014-04-27 07:58:00', 'Crusaruis28'),
('WomensSoccer', '2014-05-04 08:23:00', 'bendh18'),
('WomensSoccer', '2014-05-04 08:23:00', 'DylanNoPants'),
('WomensSoccer', '2014-05-04 08:23:00', 'Crusaruis28'),
('WomensSoccer', '2014-06-01 13:18:00', 'bendh18'),
('WomensSoccer', '2014-06-01 13:18:00', 'DylanNoPants'),
('WomensSoccer', '2014-06-01 13:18:00', 'Crusaruis28'),
('WomensSoccer', '2014-07-06 14:20:00', 'bendh18'),
('WomensSoccer', '2014-07-06 14:20:00', 'DylanNoPants'),
('WomensSoccer', '2014-07-06 14:20:00', 'Crusaruis28'),
('WomensSoccer', '2014-08-03 16:51:00', 'bendh18'),
('WomensSoccer', '2014-08-03 16:51:00', 'DylanNoPants'),
('WomensSoccer', '2014-08-03 16:51:00', 'Crusaruis28'),
('WomensSoccer', '2014-09-07 18:12:00', 'bendh18'),
('WomensSoccer', '2014-09-07 18:12:00', 'DylanNoPants'),
('WomensSoccer', '2014-09-07 18:12:00', 'Crusaruis28'),
('WomensSoccer', '2014-10-05 22:11:00', 'bendh18'),
('WomensSoccer', '2014-10-05 22:11:00', 'DylanNoPants'),
('WomensSoccer', '2014-10-05 22:11:00', 'Crusaruis28'),
('WomensSoccer', '2014-11-11 08:59:00', 'bendh18'),
('WomensSoccer', '2014-11-11 08:59:00', 'DylanNoPants'),
('WomensSoccer', '2014-11-11 08:59:00', 'Crusaruis28'),
('WomensSoccer', '2014-12-02 09:49:00', 'bendh18'),
('WomensSoccer', '2014-12-02 09:49:00', 'DylanNoPants'),
('WomensSoccer', '2014-12-02 09:49:00', 'Crusaruis28'),
('WomensSoccer', '2015-01-06 12:35:00', 'bendh18'),
('WomensSoccer', '2015-01-06 12:35:00', 'DylanNoPants'),
('WomensSoccer', '2015-01-06 12:35:00', 'Crusaruis28'),
('WomensSoccer', '2015-02-04 18:56:00', 'bendh18'),
('WomensSoccer', '2015-02-04 18:56:00', 'DylanNoPants'),
('WomensSoccer', '2015-02-04 18:56:00', 'Crusaruis28'),
('WomensSoccer', '2015-03-10 06:33:00', 'bendh18'),
('WomensSoccer', '2015-03-10 06:33:00', 'DylanNoPants'),
('WomensSoccer', '2015-03-10 06:33:00', 'Crusaruis28'),
('WomensSoccer', '2015-03-10 06:33:00', 'AutoModerator'),
('WomensSoccer', '2015-04-07 11:01:00', 'bendh18'),
('WomensSoccer', '2015-04-07 11:01:00', 'DylanNoPants'),
('WomensSoccer', '2015-04-07 11:01:00', 'Crusaruis28'),
('WomensSoccer', '2015-04-07 11:01:00', 'AutoModerator'),
('WomensSoccer', '2015-05-11 19:55:00', 'bendh18'),
('WomensSoccer', '2015-05-11 19:55:00', 'DylanNoPants'),
('WomensSoccer', '2015-05-11 19:55:00', 'Crusaruis28'),
('WomensSoccer', '2015-05-11 19:55:00', 'AutoModerator'),
('WomensSoccer', '2015-06-18 00:55:00', 'DylanNoPants'),
('WomensSoccer', '2015-06-18 00:55:00', 'Crusaruis28'),
('WomensSoccer', '2015-07-03 05:52:00', 'DylanNoPants'),
('WomensSoccer', '2015-07-03 05:52:00', 'Crusaruis28'),
('WomensSoccer', '2016-05-05 08:59:00', 'DylanNoPants'),
('WomensSoccer', '2016-05-05 08:59:00', 'Crusaruis28'),
('WomensSoccer', '2016-05-05 08:59:00', 'MercuryPDX'),
('WomensSoccer', '2016-06-02 08:47:00', 'DylanNoPants'),
('WomensSoccer', '2016-06-02 08:47:00', 'Crusaruis28'),
('WomensSoccer', '2016-06-02 08:47:00', 'MercuryPDX'),
('WomensSoccer', '2016-07-08 15:08:00', 'DylanNoPants'),
('WomensSoccer', '2016-07-08 15:08:00', 'Crusaruis28'),
('WomensSoccer', '2016-07-08 15:08:00', 'MercuryPDX'),
('WomensSoccer', '2016-08-19 11:06:00', 'DylanNoPants'),
('WomensSoccer', '2016-08-19 11:06:00', 'Crusaruis28'),
('WomensSoccer', '2016-08-19 11:06:00', 'MercuryPDX'),
('WomensSoccer', '2016-09-02 06:34:00', 'DylanNoPants'),
('WomensSoccer', '2016-09-02 06:34:00', 'Crusaruis28'),
('WomensSoccer', '2016-09-02 06:34:00', 'MercuryPDX'),
('WomensSoccer', '2016-10-07 12:46:00', 'DylanNoPants'),
('WomensSoccer', '2016-10-07 12:46:00', 'Crusaruis28'),
('WomensSoccer', '2016-10-07 12:46:00', 'MercuryPDX'),
('WomensSoccer', '2017-08-08 21:47:00', 'DylanNoPants'),
('WomensSoccer', '2017-08-08 21:47:00', 'Crusaruis28'),
('WomensSoccer', '2017-08-08 21:47:00', 'MercuryPDX'),
('WomensSoccer', '2017-11-22 23:39:00', 'DylanNoPants'),
('WomensSoccer', '2017-11-22 23:39:00', 'Crusaruis28'),
('WomensSoccer', '2017-11-22 23:39:00', 'MercuryPDX'),
('WomensSoccer', '2018-01-24 03:55:00', 'DylanNoPants'),
('WomensSoccer', '2018-01-24 03:55:00', 'Crusaruis28'),
('WomensSoccer', '2018-01-24 03:55:00', 'MercuryPDX');



grant select on s1_moderators_updates to public;
