create sequence statusesreports_id_seq
    as integer;

alter sequence statusesreports_id_seq owner to svitekas;

create table organizationroles
(
    id   serial
        primary key,
    role varchar(255)
);

comment on table organizationroles is 'Справочник ролей организаций';

comment on column organizationroles.role is 'Роль';

alter table organizationroles
    owner to svitekas;

create table statusesreports
(
    id          integer      not null
        primary key,
    name_status varchar(255) not null
);

comment on table statusesreports is 'Справочник статусов отчетов';

comment on column statusesreports.name_status is 'Наименование статуса';

alter table statusesreports
    owner to svitekas;

alter sequence statusesreports_id_seq owned by statusesreports.id;

create table themesletter
(
    id    serial
        primary key,
    theme varchar(255)
);

comment on table themesletter is 'Справочник тем писем (для рабочей переписки)';

comment on column themesletter.theme is 'Тема письма';

alter table themesletter
    owner to svitekas;

create table textforformdocument
(
    id        serial
        primary key,
    key       varchar(255),
    text_data text,
    commnet   text
);

comment on table textforformdocument is 'Текстовые предложения для форм документов';

comment on column textforformdocument.key is 'key (если будет не удобно по id обращаться)';

comment on column textforformdocument.text_data is 'Предложение для вставки в форму документа';

comment on column textforformdocument.commnet is 'Комментарий (чисто для себя)';

alter table textforformdocument
    owner to svitekas;

create table organizations
(
    id                                      serial
        primary key,
    id_users                                integer not null
        constraint fk_auth_user_to_organizations
            references public.auth_user,
    inn                                     varchar(12),
    ogrn                                    varchar(15),
    kpp                                     varchar(9),
    legal_address                           text,
    postal_address                          text,
    contact_telephone_number                varchar(20),
    short_name_organisation                 varchar(255),
    short_legal_form                        varchar(100),
    full_legal_form                         varchar(255),
    is_deleted                              boolean   default false,
    create_entry                            timestamp default CURRENT_TIMESTAMP,
    role_organisation                       integer not null
        constraint fk_organizationroles_to_organizations
            references organizationroles,
    full_name_nominative                    varchar(500),
    full_name_genitive                      varchar(500),
    full_name_dative                        varchar(500),
    full_name_accusative                    varchar(500),
    full_name_instrumental                  varchar(500),
    full_name_prepositional                 varchar(500),
    representative_signature                varchar(255),
    representative_appeal                   varchar(255),
    representative_name_nominative          varchar(255),
    representative_name_genitive            varchar(255),
    representative_name_dative              varchar(255),
    representative_name_accusative          varchar(255),
    representative_name_instrumental        varchar(255),
    representative_name_prepositional       varchar(255),
    representative_basis                    varchar(255),
    representative_short_form               varchar(255),
    position_nominative                     varchar(255),
    position_genitive                       varchar(255),
    position_dative                         varchar(255),
    position_accusative                     varchar(255),
    position_instrumental                   varchar(255),
    position_prepositional                  varchar(255),
    long_name_organisation                  text,
    representative_name_short_nominative    varchar(255),
    representative_name_short_genitive      varchar(255),
    representative_name_short_dative        varchar(255),
    representative_name_short_accusative    varchar(255),
    representative_name_short_instrumental  varchar(255),
    representative_name_short_prepositional varchar(255)
);

comment on table organizations is 'Данные по организациям';

comment on column organizations.id_users is 'id пользователя';

comment on column organizations.inn is 'ИНН';

comment on column organizations.ogrn is 'ОГРН';

comment on column organizations.kpp is 'КПП';

comment on column organizations.legal_address is 'Юридический адрес';

comment on column organizations.postal_address is 'Почтовый адрес';

comment on column organizations.contact_telephone_number is 'Контактный номер телефона';

comment on column organizations.short_name_organisation is 'Краткое наименование организации';

comment on column organizations.short_legal_form is 'Организационно-правовая форма краткая';

comment on column organizations.full_legal_form is 'Организационно-правовая форма полная';

comment on column organizations.is_deleted is 'Признак удаления';

comment on column organizations.create_entry is 'Дата и время создания записи';

comment on column organizations.role_organisation is 'id роли организации';

comment on column organizations.full_name_nominative is 'Полное наименование организации. Именительный падеж';

comment on column organizations.full_name_genitive is 'Полное наименование организации. Родительный падеж';

comment on column organizations.full_name_dative is 'Полное наименование организации. Дательный падеж';

comment on column organizations.full_name_accusative is 'Полное наименование организации. Винительный падеж';

comment on column organizations.full_name_instrumental is 'Полное наименование организации. Творительный падеж';

comment on column organizations.full_name_prepositional is 'Полное наименование организации. Предложный падеж';

comment on column organizations.representative_signature is 'Представитель организации. Краткая подпись';

comment on column organizations.representative_appeal is 'Представитель организации. Обращение';

comment on column organizations.representative_name_nominative is 'Полное наименование представителя организации. Именительный падеж';

comment on column organizations.representative_name_genitive is 'Полное наименование представителя организации. Родительный падеж';

comment on column organizations.representative_name_dative is 'Полное наименование представителя организации. Дательный падеж';

comment on column organizations.representative_name_accusative is 'Полное наименование представителя организации. Винительный падеж';

comment on column organizations.representative_name_instrumental is 'Полное наименование представителя организации. Творительный падеж';

comment on column organizations.representative_name_prepositional is 'Полное наименование представителя организации. Предложный падеж';

comment on column organizations.representative_basis is 'Представитель организации. Действует на основании';

comment on column organizations.representative_short_form is 'Представитель организации. Краткая форма записи';

comment on column organizations.position_nominative is 'Должность представителя организации. Именительный падеж';

comment on column organizations.position_genitive is 'Должность представителя организации. Родительный падеж';

comment on column organizations.position_dative is 'Должность представителя организации. Дательный падеж';

comment on column organizations.position_accusative is 'Должность представителя организации. Винительный падеж';

comment on column organizations.position_instrumental is 'Должность представителя организации. Творительный падеж';

comment on column organizations.position_prepositional is 'Должность представителя организации. Предложный падеж';

comment on column organizations.long_name_organisation is 'Длинное наименование организации с сокращенной правовой формой';

comment on column organizations.representative_name_short_nominative is 'Сокращенное ФИО представителя организации. Именительный падеж';

comment on column organizations.representative_name_short_genitive is 'Сокращенное ФИО представителя организации. Родительный падеж';

comment on column organizations.representative_name_short_dative is 'Сокращенное ФИО представителя организации. Дательный падеж';

comment on column organizations.representative_name_short_accusative is 'Сокращенное ФИО представителя организации. Винительный падеж';

comment on column organizations.representative_name_short_instrumental is 'Сокращенное ФИО представителя организации. Творительный падеж';

comment on column organizations.representative_name_short_prepositional is 'Сокращенное ФИО представителя организации. Предложный падеж';

alter table organizations
    owner to svitekas;

create index idx_organizations_id_users
    on organizations (id_users);

create index idx_organizations_role
    on organizations (role_organisation);

create table contracts
(
    id                            serial
        primary key,
    id_customer                   integer not null
        constraint fk_organizations_to_contracts_customer
            references organizations,
    id_contractor                 integer not null
        constraint fk_organizations_to_contracts_contractor
            references organizations,
    id_users                      integer not null
        constraint fk_auth_user_to_contracts
            references public.auth_user,
    date_contract                 date,
    contract_period               date,
    subject_contract              text,
    theme_contract                text,
    service_name                  text,
    is_deleted                    boolean   default false,
    create_entry                  timestamp default CURRENT_TIMESTAMP,
    goals                         text,
    tasks                         text,
    description_target_audience   text,
    requirements_visual_materials text,
    requirements_text_materials   text,
    kpi_plan_clicks               integer,
    kpi_plan_reject               numeric(5, 2),
    login_yandex_direct           text,
    number_contract               text
);

comment on table contracts is 'Данные по договорам';

comment on column contracts.id_customer is 'Заказчик';

comment on column contracts.id_contractor is 'Исполнитель';

comment on column contracts.id_users is 'id пользователя';

comment on column contracts.date_contract is 'Дата договора';

comment on column contracts.contract_period is 'Срок действия договора';

comment on column contracts.subject_contract is 'Предмет договора';

comment on column contracts.theme_contract is 'Тема договора';

comment on column contracts.service_name is 'Наименование услуги';

comment on column contracts.is_deleted is 'Признак удаления';

comment on column contracts.create_entry is 'Дата и время создания записи';

comment on column contracts.goals is 'Цели';

comment on column contracts.tasks is 'Задачи';

comment on column contracts.description_target_audience is 'Описание целевой аудитории';

comment on column contracts.requirements_visual_materials is 'Требования к креативным материалам';

comment on column contracts.requirements_text_materials is 'Требования к текстовым материалам';

comment on column contracts.kpi_plan_clicks is 'KPI: количество кликов не менее (шт.)';

comment on column contracts.kpi_plan_reject is 'KPI: доля отказов не более (в %)';

comment on column contracts.login_yandex_direct is 'Логин от кабинета Яндекс.Директ';

comment on column contracts.number_contract is 'Номер договора (внутренний)';

alter table contracts
    owner to svitekas;

create index idx_contracts_customer
    on contracts (id_customer);

create index idx_contracts_contractor
    on contracts (id_contractor);

create index idx_contracts_users
    on contracts (id_users);

create table terms
(
    id               serial
        primary key,
    id_contract      integer not null
        constraint fk_contracts_to_terms
            references contracts,
    serial_number    integer,
    term_title       text,
    term_description text,
    is_deleted       boolean   default false,
    create_entry     timestamp default CURRENT_TIMESTAMP
);

comment on table terms is 'Термины и определения';

comment on column terms.serial_number is 'Порядковый номер термина в рамках договора';

comment on column terms.term_title is 'Сам термин';

comment on column terms.term_description is 'Описание термина';

comment on column terms.is_deleted is 'Признак удаления';

comment on column terms.create_entry is 'Дата и время создания записи';

alter table terms
    owner to svitekas;

create index idx_terms_contract
    on terms (id_contract);

create table requests
(
    id                              serial
        primary key,
    id_contracts                    integer not null
        constraint fk_contracts_to_requests
            references contracts,
    id_users                        integer not null
        constraint fk_auth_user_to_requests
            references public.auth_user,
    application_number              varchar(50),
    date_request                    date,
    start_date                      date,
    end_date                        date,
    campany_yandex_direct           jsonb,
    digital_project                 text,
    list_recommended_campaign_types text,
    list_recommended_formats_ads    text,
    description_target_audience     text    not null,
    interests                       text,
    examples_published_ads          text,
    conclusions_recommendations     text,
    media_carrier_type_id           varchar(100),
    media_material_name_1           varchar(255),
    media_file_name_1               varchar(255),
    media_material_name_2           varchar(255),
    media_file_name_2               varchar(255),
    financial_unit                  varchar(50),
    financial_quantity              integer,
    financial_price_per_unit        numeric(10, 2),
    financial_total_amount          numeric(12, 2),
    amount_due_words                text,
    financial_vat_amount            numeric(12, 2),
    financial_vat_amount_words      text,
    financial_quality               varchar(255),
    is_deleted                      boolean   default false,
    create_entry                    timestamp default CURRENT_TIMESTAMP,
    advance_payment_transferred     double precision,
    advance_payment_credited        double precision,
    amount_due                      double precision,
    deleted_groups                  jsonb
);

comment on table requests is 'Данные по заявкам';

comment on column requests.id_contracts is 'id контракта';

comment on column requests.id_users is 'id пользователя';

comment on column requests.application_number is 'Номер заявки';

comment on column requests.date_request is 'Дата заявки';

comment on column requests.start_date is 'Дата начала';

comment on column requests.end_date is 'Дата окончания';

comment on column requests.campany_yandex_direct is 'Идентификаторы и наименования кампаний из Яндекс.Директа';

comment on column requests.digital_project is 'Цифровой проект и/или сервис города Москвы';

comment on column requests.list_recommended_campaign_types is 'Перечень рекомендуемых типов кампаний';

comment on column requests.list_recommended_formats_ads is 'Перечень рекомендуемых форматов объявлений';

comment on column requests.description_target_audience is 'Описание целевой аудитории';

comment on column requests.interests is 'Интересы';

comment on column requests.examples_published_ads is 'Примеры опубликованных объявлений,разработанные в рамках Заявки';

comment on column requests.conclusions_recommendations is 'Выводы и рекомендации по проведению контекстных рекламных кампаний:';

comment on column requests.media_carrier_type_id is 'Ведомость машинных носителей. Тип и №/ID носителя';

comment on column requests.media_material_name_1 is 'Ведомость машинных носителей. Наименование материала 1';

comment on column requests.media_file_name_1 is 'Ведомость машинных носителей. Имя файла 1';

comment on column requests.media_material_name_2 is 'Ведомость машинных носителей. Наименование материала 2';

comment on column requests.media_file_name_2 is 'Ведомость машинных носителей. Имя файла 2';

comment on column requests.financial_unit is 'Финансовые данные. Единицы измерения';

comment on column requests.financial_quantity is 'Финансовые данные. Количество';

comment on column requests.financial_price_per_unit is 'Финансовые данные. Цена за ед. (руб.), в том числе НДС (7%)';

comment on column requests.financial_total_amount is 'Финансовые данные. Сумма (в руб.), в том числе НДС (7%)';

comment on column requests.amount_due_words is 'Финансовые данные. Сумма (в руб.), в том числе НДС (7%) прописью';

comment on column requests.financial_vat_amount is 'Финансовые данные. Сумма НДС (7%)';

comment on column requests.financial_vat_amount_words is 'Финансовые данные. Сумма НДС (7%) прописью';

comment on column requests.financial_quality is 'Финансовые данные. Качество';

comment on column requests.is_deleted is 'Признак удаления';

comment on column requests.create_entry is 'Дата и время создания записи';

comment on column requests.advance_payment_transferred is 'Перечисленный аванс';

comment on column requests.advance_payment_credited is 'Зачтенный аванс';

comment on column requests.amount_due is 'сумма подлежащая уплате (к ней и расшифровку указываем текстовую)';

comment on column requests.deleted_groups is 'Удаленные группы у кампаний Директа (которые не войдут в отчет)';

alter table requests
    owner to svitekas;

create index idx_requests_contracts
    on requests (id_contracts);

create index idx_requests_users
    on requests (id_users);

create table "workСorrespondence"
(
    id             serial
        primary key,
    id_requests    integer not null
        constraint "fk_requests_to_workСorrespondence"
            references requests,
    serial_number  integer,
    date_sent      date,
    id_letter_name integer not null
        constraint "fk_themesletter_to_workСorrespondence"
            references themesletter,
    file_link      text,
    is_deleted     boolean   default false,
    create_entry   timestamp default CURRENT_TIMESTAMP
);

comment on table "workСorrespondence" is 'Файлы рабочей переписки';

comment on column "workСorrespondence".id_requests is 'id заявки';

comment on column "workСorrespondence".serial_number is 'Порядковый номер (в рамках одной заявки)';

comment on column "workСorrespondence".date_sent is 'Дата письма';

comment on column "workСorrespondence".id_letter_name is 'id темы письма';

comment on column "workСorrespondence".file_link is 'Ссылка на файл';

comment on column "workСorrespondence".is_deleted is 'Признак удаления файла';

comment on column "workСorrespondence".create_entry is 'Дата и время создания записи';

alter table "workСorrespondence"
    owner to svitekas;

create index "idx_workСorrespondence_requests"
    on "workСorrespondence" (id_requests);

create index "idx_workСorrespondence_letter_name"
    on "workСorrespondence" (id_letter_name);

create table reports
(
    id                             serial
        primary key,
    id_requests                    integer not null
        constraint fk_requests_to_reports
            references requests,
    id_contracts                   integer not null
        constraint fk_contracts_to_reports
            references contracts,
    id_users                       integer not null
        constraint fk_auth_user_to_reports
            references public.auth_user,
    id_status                      integer not null
        constraint fk_statusesreports_to_reports
            references statusesreports,
    message                        text,
    select_content_report          boolean   default false,
    select_screenshots_ads         boolean   default false,
    select_machine_media_statement boolean   default false,
    select_presentation_keys       boolean   default false,
    select_media_plan              boolean   default false,
    select_cover_letter            boolean   default false,
    select_act                     boolean   default false,
    file_content_report            text,
    file_screenshots_ads           text,
    file_machine_media_statement   text,
    file_presentation_keys         text,
    file_media_plan                text,
    file_cover_letter              text,
    file_act                       text,
    file_archive_all               text,
    is_deleted                     boolean   default false,
    create_entry                   timestamp default CURRENT_TIMESTAMP
);

comment on table reports is 'Данные по отчетам';

comment on column reports.id_requests is 'id заявки';

comment on column reports.id_contracts is 'id договора';

comment on column reports.id_users is 'id пользователя';

comment on column reports.id_status is 'id статуса отчета';

comment on column reports.message is 'Сообщение для отображения на фронте (ошибка, успех и т.п.)';

comment on column reports.select_content_report is 'Признак выбора. Содержательный отчет (.docx)';

comment on column reports.select_screenshots_ads is 'Признак выбора. Скриншоты объявлений (.rar)';

comment on column reports.select_machine_media_statement is 'Признак выбора. Ведомость машинных носителей информации (.docx)';

comment on column reports.select_presentation_keys is 'Признак выбора. Презентация с группами ключей (.pptx)';

comment on column reports.select_media_plan is 'Признак выбора. Медиаплан (.xlsx)';

comment on column reports.select_cover_letter is 'Признак выбора. Сопровод (.docx)';

comment on column reports.select_act is 'Признак выбора. Акт (.docx)';

comment on column reports.file_content_report is 'Ссылка на файл. Содержательный отчет (.docx)';

comment on column reports.file_screenshots_ads is 'Ссылка на файл. Скриншоты объявлений (.rar)';

comment on column reports.file_machine_media_statement is 'Ссылка на файл. Ведомость машинных носителей информации (.docx)';

comment on column reports.file_presentation_keys is 'Ссылка на файл. Презентация с группами ключей (.pptx)';

comment on column reports.file_media_plan is 'Ссылка на файл. Медиаплан (.xlsx)';

comment on column reports.file_cover_letter is 'Ссылка на файл. Сопровод (.docx)';

comment on column reports.file_act is 'Ссылка на файл. Акт (.docx)';

comment on column reports.file_archive_all is 'Ссылка на файл. Общий архив со всеми выбранными файлами';

comment on column reports.is_deleted is 'Признак удаления';

comment on column reports.create_entry is 'Дата и время создания записи';

alter table reports
    owner to svitekas;

create index idx_reports_requests
    on reports (id_requests);

create index idx_reports_contracts
    on reports (id_contracts);

create index idx_reports_users
    on reports (id_users);

create index idx_reports_status
    on reports (id_status);

create table yandexdirectaccounts
(
    id               serial
        primary key,
    direct_api_token text         not null,
    client_id        varchar(255) not null,
    client_secret    varchar(255) not null,
    is_deleted       boolean   default false,
    comment          text,
    create_entry     timestamp default CURRENT_TIMESTAMP,
    update_entry     timestamp default CURRENT_TIMESTAMP
);

comment on table yandexdirectaccounts is 'Аккаунты Яндекс.Директа для доступа к API';

comment on column yandexdirectaccounts.id is 'Уникальный идентификатор записи';

comment on column yandexdirectaccounts.direct_api_token is 'Токен доступа к API Яндекс.Директа';

comment on column yandexdirectaccounts.client_id is 'CLIENT_ID для OAuth авторизации';

comment on column yandexdirectaccounts.client_secret is 'CLIENT_SECRET для OAuth авторизации';

comment on column yandexdirectaccounts.is_deleted is 'Признак удаления записи';

comment on column yandexdirectaccounts.comment is 'Комментарий к аккаунту';

comment on column yandexdirectaccounts.create_entry is 'Дата и время создания записи';

comment on column yandexdirectaccounts.update_entry is 'Дата и время последнего обновления записи';

alter table yandexdirectaccounts
    owner to svitekas;

create index idx_yandexdirectaccounts_client_id
    on yandexdirectaccounts (client_id);

create index idx_yandexdirectaccounts_is_deleted
    on yandexdirectaccounts (is_deleted);

create table wordstatapiaccounts
(
    id             serial
        primary key,
    wordstat_login varchar(255) not null,
    wordstat_token text         not null,
    client_id      varchar(255) not null,
    client_secret  varchar(255) not null,
    is_deleted     boolean   default false,
    comment        text,
    create_entry   timestamp default CURRENT_TIMESTAMP,
    update_entry   timestamp default CURRENT_TIMESTAMP
);

comment on table wordstatapiaccounts is 'Аккаунты Wordstat API для доступа к сервису статистики поисковых запросов';

comment on column wordstatapiaccounts.id is 'Уникальный идентификатор записи';

comment on column wordstatapiaccounts.wordstat_login is 'Логин для доступа к Wordstat API';

comment on column wordstatapiaccounts.wordstat_token is 'Токен доступа к API Wordstat';

comment on column wordstatapiaccounts.client_id is 'CLIENT_ID для OAuth авторизации';

comment on column wordstatapiaccounts.client_secret is 'CLIENT_SECRET для OAuth авторизации';

comment on column wordstatapiaccounts.is_deleted is 'Признак удаления записи';

comment on column wordstatapiaccounts.comment is 'Комментарий к аккаунту';

comment on column wordstatapiaccounts.create_entry is 'Дата и время создания записи';

comment on column wordstatapiaccounts.update_entry is 'Дата и время последнего обновления записи';

alter table wordstatapiaccounts
    owner to svitekas;

create index idx_wordstatapiaccounts_login
    on wordstatapiaccounts (wordstat_login);

create index idx_wordstatapiaccounts_client_id
    on wordstatapiaccounts (client_id);

create index idx_wordstatapiaccounts_is_deleted
    on wordstatapiaccounts (is_deleted);

create table wordstatkeyphrases
(
    id           serial
        primary key,
    phrase       text not null,
    regions      text,
    devices      text,
    is_deleted   boolean   default false,
    create_entry timestamp default CURRENT_TIMESTAMP,
    count        integer
);

comment on table wordstatkeyphrases is 'Ключевые фразы из Wordstat для анализа поисковых запросов';

comment on column wordstatkeyphrases.id is 'Уникальный идентификатор записи';

comment on column wordstatkeyphrases.phrase is 'Ключевая фраза из Wordstat';

comment on column wordstatkeyphrases.regions is 'Номера регионов (массив) или null если все регионы';

comment on column wordstatkeyphrases.devices is 'Список типов устройств, с которых был задан запрос';

comment on column wordstatkeyphrases.is_deleted is 'Признак удаления записи';

comment on column wordstatkeyphrases.create_entry is 'Дата и время создания записи';

comment on column wordstatkeyphrases.count is 'Количество запросов по данной фразе за предыдущий месяц';

alter table wordstatkeyphrases
    owner to svitekas;

create index idx_wordstatkeyphrases_phrase
    on wordstatkeyphrases (phrase);

create index idx_wordstatkeyphrases_is_deleted
    on wordstatkeyphrases (is_deleted);

create index idx_wordstatkeyphrases_create_entry
    on wordstatkeyphrases (create_entry);

create unique index idx_wordstatkeyphrases_phrase_unique
    on wordstatkeyphrases (phrase)
    where (is_deleted = false);

create table projects
(
    id           serial
        primary key,
    name         text not null,
    link         text,
    comment      text,
    is_deleted   boolean   default false,
    create_entry timestamp default CURRENT_TIMESTAMP
);

comment on table projects is 'Проекты';

comment on column projects.id is 'Уникальный идентификатор записи';

comment on column projects.name is 'Название проекта';

comment on column projects.link is 'Ссылка на проект';

comment on column projects.comment is 'Комментарий к проекту';

comment on column projects.is_deleted is 'Признак удаления записи';

comment on column projects.create_entry is 'Дата и время создания записи';

alter table projects
    owner to svitekas;

create index idx_projects_name
    on projects (name);

create index idx_projects_is_deleted
    on projects (is_deleted);

create index idx_projects_create_entry
    on projects (create_entry);

create function update_yandex_direct_accounts_timestamp() returns trigger
    language plpgsql
as
$$
BEGIN
    NEW.update_entry = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$;

alter function update_yandex_direct_accounts_timestamp() owner to svitekas;

create trigger update_yandex_direct_accounts_timestamp
    before update
    on yandexdirectaccounts
    for each row
execute procedure update_yandex_direct_accounts_timestamp();

create function update_wordstat_api_accounts_timestamp() returns trigger
    language plpgsql
as
$$
BEGIN
    NEW.update_entry = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$;

alter function update_wordstat_api_accounts_timestamp() owner to svitekas;

create trigger update_wordstat_api_accounts_timestamp
    before update
    on wordstatapiaccounts
    for each row
execute procedure update_wordstat_api_accounts_timestamp();

