import pandas as pd


df = pd.read_csv('/home/ADMSK/dasadovenk/python_projects/research/research/appmet/app_metrica_4035787-1691672880560.csv')

res = df[
    (df['event_name'] == 'popup_close') &
    (df['event_json'].str.contains('pin_settings')) &
    (df['event_json'].str.contains('"button_text":"save"')) &
    (df['event_json'].str.contains('"ask_pin":"true"')) &
    (df['event_json'].str.contains('"profile_choose":"false"'))
].to_dict(orient='records')

[{'event_datetime': '2023-08-10 12:05:05',
  'event_json': '{"huawei_subscriber_id":"prod.437e9030af2d40ffb8b308e64f0","experiments":["expid:0_a","posters_redesign:exp","auth_type:websso","onboarding:rebranding","original_videobanner_enabled:true","similar_shelf_source:mgw","glagne_filter:yes","morda_api_provider:mgw","original_videoshelf_enabled:true","OriginalVideoShelfPreloadDelay:1000","ios_arch_version:new","ecosystem_profile:1","refresh_token_enabled:1","AI_Voices_similar_enable:true","Autoplay_similar_movies_enable:true","moneta_purchase_button_on_main_enabled:false","company_website_enabled:false","connect_device_enabled:true","tnps_enabled:true","Auth_delete_acc_enabled:true","Auth_vpn_popup_show:1","cold_warm_start_enabled:1","connect_device_onboarding:true","moneta_avod_disable_exp:false","new_video_card_ios_arch:new","attach_file_to_feedback_enabled:true","moneta_content_quantity_enabled:1","title_controls_time:10","next_movie_switch_delay:10","Ai_Voices_next_serials_switch_delay:20","max_number_movie_switch:3","Ai_Voices_autoplay_serials_enable:true","must_auth:1","waterbase_enabled:true","player_tv_subtitles:false","Auth_ios_refactored_login:true","ds_mgw_shkurka_enabled:false","vitrina_long_click_menu_ios:1","Auth_animation_suc:1","player_use_new_huawei_device_model:false","Auth_pin_onboarding:true","Auth_start_profile_choose:true","vitrina_check_rkn_pin:false"],"button_id":"save","button_text":"save","popup_action":{"ask_pin":"true","profile_choose":"false"},"waterbase_device_id":"E1B47920-9A2F-4E8B-AC0C-1D65BAF312E6","profile_id":"prod.437e9030af2d40ffb8b308e64f0","profile_age":"21","popup_name":"pin_settings"}',
  'event_name': 'popup_close',
  'event_receive_datetime': '2023-08-10 12:05:25',
  'event_receive_timestamp': 1691658325,
  'event_timestamp': 1691658305,
  'session_id': 10000000002,
  'installation_id': '36cf3c43199941988b064806828c0066',
  'appmetrica_device_id': 6.640654430998064e+18,
  'city': 'Saint Petersburg',
  'connection_type': 'cell',
  'country_iso_code': 'RU',
  'device_ipv6': '2a00:1fa0:c695:e1c6:a1ad:f23c:4c46:8a77',
  'device_locale': 'ru_RU',
  'device_manufacturer': 'Apple',
  'device_model': 'iPhone 13 Mini',
  'device_type': 'phone',
  'google_aid': nan,
  'ios_ifa': nan,
  'ios_ifv': 'E1B47920-9A2F-4E8B-AC0C-1D65BAF312E6',
  'mcc': 0,
  'mnc': 0,
  'operator_name': nan,
  'original_device_model':'iPhone14,4',
  'os_name': 'ios',
  'os_version': '16.5',
  'profile_id': 'prod.437e9030af2d40ffb8b308e64f0',
  'windows_aid': nan,
  'app_build_number': 422,
  'app_package_name': 'ru.mts.tvh',
  'app_version_name': 3.81,
  'application_id': 4035787}
]
