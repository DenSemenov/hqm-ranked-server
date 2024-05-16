use crate::hqm_game::{
    HQMGame, HQMGameWorld, HQMObjectIndex, HQMPhysicsConfiguration, HQMPuck, HQMRinkFaceoffSpot,
    HQMRinkLine, HQMRinkSide, HQMRulesState, HQMTeam,
};
use crate::hqm_server::{HQMServer, HQMServerPlayer, HQMServerPlayerIndex, HQMServerPlayerList};
use crate::hqm_server::{HQMServerPlayerData, HQMSpawnPoint};
use crate::hqm_simulate::HQMSimulationEvent;
use itertools::Itertools;
use nalgebra::{Point3, Rotation3, Vector3};
use rand::prelude::SliceRandom;
use reqwest::{self};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::f32::consts::FRAC_PI_2;
use tracing::info;

#[derive(Serialize, Deserialize, Debug)]
pub struct APILoginResponse {
    pub id: i32,
    pub success: bool,
    pub errorMessage: String,
    pub oldNickname: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct APIGameStartPlayerResponse {
    pub id: i32,
    pub score: i32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct APIGameStartResponse {
    pub gameId: String,
    pub players: Vec<APIGameStartPlayerResponse>,
    pub captainRed: i32,
    pub captainBlue: i32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GameStartRequest {
    pub token: String,
    pub maxCount: usize,
    pub playerIds: Vec<i32>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PickRequest {
    pub token: String,
    pub gameId: String,
    pub playerId: i32,
    pub team: i32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CallGoalRequest {
    pub token: String,
    pub gameId: String,
    pub team: i32,
    pub scorer: i32,
    pub assist: i32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SaveGameRequest {
    pub token: String,
    pub gameId: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SaveGameResponse {
    pub mvp: String,
    pub players: Vec<SaveGamePlayerResponse>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SaveGamePlayerResponse {
    pub id: i32,
    pub score: i32,
    pub total: i32,
    pub pos: i32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct HeartbeatRequest {
    pub token: String,
    pub name: String,
    pub loggedIn: usize,
    pub teamMax: usize,
    pub period: u32,
    pub time: u32,
    pub redScore: u32,
    pub blueScore: u32,
    pub state: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ReportRequest {
    pub token: String,
    pub fromId: i32,
    pub toId: i32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ReportResponse {
    pub message: String,
    pub success: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ResignRequest {
    pub token: String,
    pub gameId: String,
    pub team: i32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ResetRequest {
    pub token: String,
    pub gameId: String,
}

#[derive(Clone)]
pub struct RHQMQueuePlayer {
    pub player_id: i32,
    pub player_name: String,
    pub player_index: HQMServerPlayerIndex,
    pub afk: bool,
}

#[derive(Eq, PartialEq, Debug)]
pub enum State {
    Waiting {
        waiting_for_response: bool,
    },
    CaptainsPicking {
        time_left: u32,
        options: Vec<(String, String, i32, i32)>,
        current_team: HQMTeam,
    },
    Game {
        paused: bool,
    },
}
#[derive(Clone)]
pub enum Vote {
    Resign {
        player_indexes: Vec<HQMServerPlayerIndex>,
        team: HQMTeam,
    },
    Kick {
        player_indexes: Vec<HQMServerPlayerIndex>,
        kick: HQMServerPlayerIndex,
    },
    Mute {
        player_indexes: Vec<HQMServerPlayerIndex>,
    },
    Reset {
        player_indexes: Vec<HQMServerPlayerIndex>,
    },
    None,
}

pub enum RHQMGameGoalie {
    TakeTurns {
        first: String,
        second: String,
        third: String,
        overtime: String,
    },
    Fixed {
        goalie: String,
    },
    Undefined,
}

#[derive(Clone)]
pub struct GameEvent {
    pub player_id: i32,
    pub type_id: u32,
    pub speed: f32,
    pub time: u32,
    pub period: u32,
}

#[derive(Clone)]
pub struct RHQMGamePlayer {
    pub player_id: i32,
    pub player_name: String,
    pub player_index: Option<HQMServerPlayerIndex>,
    pub player_team: Option<HQMTeam>,
    pub rating: i32,
}

pub enum ApiResponse {
    LoginFailed {
        player_index: HQMServerPlayerIndex,
        error_message: String,
    },
    LoginSuccessful {
        player_id: i32,
        player_index: HQMServerPlayerIndex,
        old_nickname: String,
    },
    GameStarted {
        gameId: String,
        players: Vec<APIGameStartPlayerResponse>,
        captainRed: i32,
        captainBlue: i32,
    },
    GameEnded {
        mvp: String,
        players: Vec<SaveGamePlayerResponse>,
    },
    Report {
        message: String,
        success: bool,
        player_index: HQMServerPlayerIndex,
    },
    Error {
        error_message: String,
    },
}

pub struct RHQMGame {
    pub(crate) notify_timer: usize,

    pub(crate) need_to_send: bool,

    pub(crate) game_players: Vec<RHQMGamePlayer>,
    pub events: Vec<GameEvent>,

    pub(crate) xpoints: Vec<f32>,
    pub(crate) zpoints: Vec<f32>,
    pub(crate) data_saved: bool,

    pub rejoin_timer: HashMap<HQMServerPlayerIndex, u32>,

    pub(crate) red_captain: Option<i32>,
    pub(crate) blue_captain: Option<i32>,

    pub(crate) red_goalie: RHQMGameGoalie,
    pub(crate) blue_goalie: RHQMGameGoalie,

    pub(crate) pick_number: usize,
}

fn limit(s: &str) -> String {
    if s.len() <= 15 {
        s.to_string()
    } else {
        let s = s[0..13].trim();
        format!("{}..", s)
    }
}

impl RHQMGame {
    pub(crate) fn get_player_by_index(
        &self,
        player_index: HQMServerPlayerIndex,
    ) -> Option<&RHQMGamePlayer> {
        self.game_players
            .iter()
            .find(|x| x.player_index == Some(player_index))
    }

    pub fn get_player_by_index_mut(
        &mut self,
        player_index: HQMServerPlayerIndex,
    ) -> Option<&mut RHQMGamePlayer> {
        self.game_players
            .iter_mut()
            .find(|x| x.player_index == Some(player_index))
    }

    pub(crate) fn get_player_by_id(&self, player_id: i32) -> Option<&RHQMGamePlayer> {
        self.game_players.iter().find(|x| x.player_id == player_id)
    }

    pub(crate) fn get_player_by_id_mut(&mut self, player_id: i32) -> Option<&mut RHQMGamePlayer> {
        self.game_players
            .iter_mut()
            .find(|x| x.player_id == player_id)
    }

    pub(crate) fn new() -> Self {
        Self {
            notify_timer: 0,

            need_to_send: false,
            game_players: Vec::new(),

            xpoints: Vec::new(),
            zpoints: Vec::new(),
            data_saved: false,

            rejoin_timer: Default::default(),
            red_captain: None,
            blue_captain: None,

            red_goalie: RHQMGameGoalie::Undefined,
            blue_goalie: RHQMGameGoalie::Undefined,
            events: Vec::new(),

            pick_number: 0,
        }
    }
}

#[derive(Eq, PartialEq, Debug, Copy, Clone)]
pub enum RankedPickingMode {
    ServerPick,
    CaptainsPick,
}

pub struct HQMRankedConfiguration {
    pub time_period: u32,
    pub time_warmup: u32,
    pub time_break: u32,
    pub time_intermission: u32,
    pub mercy: u32,
    pub first_to: u32,
    pub periods: u32,
    pub offside: HQMOffsideConfiguration,
    pub icing: HQMIcingConfiguration,
    pub offside_line: HQMOffsideLineConfiguration,
    pub twoline_pass: HQMTwoLinePassConfiguration,
    pub warmup_pucks: usize,
    pub physics_config: HQMPhysicsConfiguration,
    pub blue_line_location: f32,
    pub use_mph: bool,
    pub goal_replay: bool,

    pub picking_mode: RankedPickingMode,
    pub notification: bool,
    pub team_max: usize,

    pub server_name: String,

    pub api: String,
    pub token: String,

    pub delay: u32,
    pub faceoff_shift: bool,
}

pub enum HQMRankedEvent {
    Goal {
        team: HQMTeam,
        goal: Option<HQMServerPlayerIndex>,
        assist: Option<HQMServerPlayerIndex>,
        speed: Option<f32>, // Raw meter/game tick (so meter per 1/100 of a second)
        speed_across_line: f32,
        time: u32,
        period: u32,
    },
}

pub struct HQMRanked {
    pub config: HQMRankedConfiguration,
    pub paused: bool,
    pub(crate) pause_timer: u32,
    is_pause_goal: bool,
    next_faceoff_spot: HQMRinkFaceoffSpot,
    icing_status: HQMIcingStatus,
    offside_status: HQMOffsideStatus,
    twoline_pass_status: HQMTwoLinePassStatus,
    pass: Option<HQMPass>,
    pub(crate) preferred_positions: HashMap<HQMServerPlayerIndex, String>,

    pub started_as_goalie: Vec<HQMServerPlayerIndex>,
    faceoff_game_step: u32,
    step_where_period_ended: u32,
    too_late_printed_this_period: bool,
    start_next_replay: Option<(u32, u32, Option<HQMServerPlayerIndex>)>,
    puck_touches: HashMap<HQMObjectIndex, VecDeque<HQMPuckTouch>>,

    pub verified_players: HashMap<HQMServerPlayerIndex, i32>,
    pub queued_players: Vec<RHQMQueuePlayer>,
    pub status: State,

    pub rhqm_game: RHQMGame,

    pub tick: u32,

    pub delay_timer: u32,

    pub(crate) sender: crossbeam_channel::Sender<ApiResponse>,
    pub(crate) receiver: crossbeam_channel::Receiver<ApiResponse>,
}

impl HQMRanked {
    pub fn new(config: HQMRankedConfiguration) -> Self {
        let (sender, receiver) = crossbeam_channel::unbounded();
        let delay_timer = config.delay.clone() * 100;
        Self {
            config,
            paused: true,
            pause_timer: 0,
            is_pause_goal: false,
            next_faceoff_spot: HQMRinkFaceoffSpot::Center,
            icing_status: HQMIcingStatus::No,
            offside_status: HQMOffsideStatus::Neutral,
            twoline_pass_status: HQMTwoLinePassStatus::No,
            pass: None,
            preferred_positions: HashMap::new(),
            started_as_goalie: vec![],
            faceoff_game_step: 0,
            too_late_printed_this_period: false,
            step_where_period_ended: 0,
            start_next_replay: None,
            puck_touches: Default::default(),

            verified_players: Default::default(),
            queued_players: vec![],
            status: State::Waiting {
                waiting_for_response: false,
            },

            tick: 0,
            delay_timer: delay_timer * 100,
            rhqm_game: RHQMGame::new(),

            sender,
            receiver,
        }
    }

    pub fn clear_started_goalie(&mut self, player_index: HQMServerPlayerIndex) {
        if let Some(x) = self
            .started_as_goalie
            .iter()
            .position(|x| *x == player_index)
        {
            self.started_as_goalie.remove(x);
        }
    }

    fn do_faceoff(&mut self, server: &mut HQMServer) {
        let positions = get_faceoff_positions(
            &server.players,
            &self.preferred_positions,
            &server.game.world,
        );

        server.game.world.clear_pucks();
        self.puck_touches.clear();

        let next_faceoff_spot = server
            .game
            .world
            .rink
            .get_faceoff_spot(self.next_faceoff_spot)
            .clone();

        let puck_pos = next_faceoff_spot.center_position + &(1.5f32 * Vector3::y());

        server
            .game
            .world
            .create_puck_object(puck_pos, Rotation3::identity());

        self.started_as_goalie.clear();
        for (player_index, (team, faceoff_position)) in positions {
            let (player_position, player_rotation) = match team {
                HQMTeam::Red => next_faceoff_spot.red_player_positions[&faceoff_position].clone(),
                HQMTeam::Blue => next_faceoff_spot.blue_player_positions[&faceoff_position].clone(),
            };
            server.spawn_skater(player_index, team, player_position, player_rotation);
            if faceoff_position == "G" {
                self.started_as_goalie.push(player_index);
            }
        }

        let rink = &server.game.world.rink;
        self.icing_status = HQMIcingStatus::No;
        self.offside_status = if rink
            .red_lines_and_net
            .offensive_line
            .point_past_middle_of_line(&puck_pos)
        {
            HQMOffsideStatus::InOffensiveZone(HQMTeam::Red)
        } else if rink
            .blue_lines_and_net
            .offensive_line
            .point_past_middle_of_line(&puck_pos)
        {
            HQMOffsideStatus::InOffensiveZone(HQMTeam::Blue)
        } else {
            HQMOffsideStatus::Neutral
        };
        self.twoline_pass_status = HQMTwoLinePassStatus::No;
        self.pass = None;

        self.faceoff_game_step = server.game.game_step;

        if self.config.faceoff_shift == false {
            server.game.world.physics_config.shift_enabled = false;
        }
    }

    pub(crate) fn update_game_over(&mut self, server: &mut HQMServer) {
        let time_gameover = self.config.time_intermission * 100;
        let time_break = self.config.time_break * 100;

        let red_score = server.game.red_score;
        let blue_score = server.game.blue_score;
        let old_game_over = server.game.game_over;
        server.game.game_over =
            if server.game.period > self.config.periods && red_score != blue_score {
                true
            } else if self.config.mercy > 0
                && (red_score.saturating_sub(blue_score) >= self.config.mercy
                    || blue_score.saturating_sub(red_score) >= self.config.mercy)
            {
                true
            } else if self.config.first_to > 0
                && (red_score >= self.config.first_to || blue_score >= self.config.first_to)
            {
                true
            } else {
                false
            };
        if server.game.game_over && !old_game_over {
            self.pause_timer = self.pause_timer.max(time_gameover);
        } else if !server.game.game_over && old_game_over {
            self.pause_timer = self.pause_timer.max(time_break);
        }
    }

    fn call_goal(
        &mut self,
        server: &mut HQMServer,
        team: HQMTeam,
        puck_index: HQMObjectIndex,
    ) -> HQMRankedEvent {
        let time_break = self.config.time_break * 100;

        match team {
            HQMTeam::Red => {
                server.game.red_score += 1;
            }
            HQMTeam::Blue => {
                server.game.blue_score += 1;
            }
        };

        self.next_faceoff_spot = HQMRinkFaceoffSpot::Center;
        let mut scorer = -1;
        let mut assist = -1;
        let (
            goal_scorer_index,
            assist_index,
            puck_speed_across_line,
            puck_speed_from_stick,
            last_touch,
        ) = if let Some(this_puck) = server.game.world.objects.get_puck_mut(puck_index) {
            let mut goal_scorer_index = None;
            let mut assist_index = None;
            let mut goal_scorer_first_touch = 0;
            let mut puck_speed_from_stick = None;
            let mut last_touch = None;
            let puck_speed_across_line = this_puck.body.linear_velocity.norm();
            if let Some(touches) = self.puck_touches.get(&puck_index) {
                last_touch = touches.front().map(|x| x.player_index);

                for touch in touches.iter() {
                    if goal_scorer_index.is_none() {
                        if touch.team == team {
                            goal_scorer_index = Some(touch.player_index);
                            goal_scorer_first_touch = touch.first_time;
                            puck_speed_from_stick = Some(touch.puck_speed);

                            let scoring_rhqm_player =
                                self.rhqm_game.get_player_by_index_mut(touch.player_index);

                            if let Some(scoring_rhqm_player) = scoring_rhqm_player {
                                scorer = scoring_rhqm_player.player_id;

                                let x = this_puck.body.linear_velocity.norm();

                                let speed = ((x * 100.0 * 3.6) * 100.0).round() / 100.0;

                                let event = GameEvent {
                                    player_id: scoring_rhqm_player.player_id,
                                    type_id: 1,
                                    speed,
                                    time: server.game.time,
                                    period: server.game.period,
                                };

                                self.rhqm_game.events.push(event);
                            }
                        }
                    } else {
                        if touch.team == team {
                            if Some(touch.player_index) == goal_scorer_index {
                                goal_scorer_first_touch = touch.first_time;
                            } else {
                                // This is the first player on the scoring team that touched it apart from the goal scorer
                                // If more than 10 seconds passed between the goal scorer's first touch
                                // and this last touch, it doesn't count as an assist

                                let diff = touch.last_time.saturating_sub(goal_scorer_first_touch);

                                if diff <= 1000 {
                                    assist_index = Some(touch.player_index);

                                    let assist_rhqm_player =
                                        self.rhqm_game.get_player_by_index_mut(touch.player_index);

                                    if let Some(assist_rhqm_player) = assist_rhqm_player {
                                        assist = assist_rhqm_player.player_id;
                                    }
                                }
                                break;
                            }
                        }
                    }
                }
            }

            (
                goal_scorer_index,
                assist_index,
                puck_speed_across_line,
                puck_speed_from_stick,
                last_touch,
            )
        } else {
            (None, None, 0.0, None, None)
        };

        let api = self.config.api.clone();
        let token = self.config.token.clone();
        let game_id = server.game.game_id.clone();
        let scorer_id = scorer;
        let assist_id = assist;
        let mut scorer_team = 0;
        if team == HQMTeam::Blue {
            scorer_team = 1;
        }
        let client = server.reqwest_client.clone();

        tokio::spawn(async move {
            let url = format!("{}/api/Server/AddGoal", api);

            let request = CallGoalRequest {
                token: token,
                gameId: game_id,
                team: scorer_team,
                scorer: scorer_id,
                assist: assist_id,
            };

            let _response: reqwest::Response =
                client.post(url).json(&request).send().await.unwrap();

            Ok::<_, anyhow::Error>(())
        });

        server
            .messages
            .add_goal_message(team, goal_scorer_index, assist_index);

        fn convert(puck_speed: f32, use_mph: bool) -> (f32, &'static str) {
            if use_mph {
                (puck_speed * 100f32 * 2.23693, "mph")
            } else {
                (puck_speed * 100f32 * 3.6, "km/h")
            }
        }

        let (puck_speed_across_line_converted, puck_speed_unit) =
            convert(puck_speed_across_line, self.config.use_mph);

        let str1 = format!(
            "Goal scored, {:.1} {} across line",
            puck_speed_across_line_converted, puck_speed_unit
        );

        let str2 = if let Some(puck_speed_from_stick) = puck_speed_from_stick {
            let (puck_speed_converted, puck_speed_unit) =
                convert(puck_speed_from_stick, self.config.use_mph);
            format!(
                ", {:.1} {} from stick",
                puck_speed_converted, puck_speed_unit
            )
        } else {
            "".to_owned()
        };
        let s = format!("{}{}", str1, str2);

        server.messages.add_server_chat_message(s);

        if server.game.time < 1000 {
            let time = server.game.time;
            let seconds = time / 100;
            let centi = time % 100;

            let s = format!("{}.{:02} seconds left", seconds, centi);
            server.messages.add_server_chat_message(s);
        }

        self.pause_timer = time_break;
        self.is_pause_goal = true;

        self.update_game_over(server);

        let gamestep = server.game.game_step;

        if self.config.goal_replay {
            let force_view = goal_scorer_index.or(last_touch);
            self.start_next_replay = Some((
                self.faceoff_game_step.max(gamestep - 600),
                gamestep + 200,
                force_view,
            ));

            self.pause_timer = self.pause_timer.saturating_sub(800).max(400);
        }
        HQMRankedEvent::Goal {
            team,
            time: server.game.time,
            period: server.game.period,
            goal: goal_scorer_index,
            assist: assist_index,
            speed: puck_speed_from_stick,
            speed_across_line: puck_speed_across_line,
        }
    }

    fn handle_events_end_of_period(
        &mut self,
        server: &mut HQMServer,
        events: &[HQMSimulationEvent],
    ) {
        for event in events {
            if let HQMSimulationEvent::PuckEnteredNet { .. } = event {
                let time = server
                    .game
                    .game_step
                    .saturating_sub(self.step_where_period_ended);
                if time <= 300 && !self.too_late_printed_this_period {
                    let seconds = time / 100;
                    let centi = time % 100;
                    self.too_late_printed_this_period = true;
                    let s = format!("{}.{:02} seconds too late!", seconds, centi);

                    server.messages.add_server_chat_message(s);
                }
            }
        }
    }

    fn handle_puck_touch(
        &mut self,
        server: &mut HQMServer,
        player: HQMObjectIndex,
        puck_index: HQMObjectIndex,
    ) {
        server.game.world.physics_config.shift_enabled = true;
        if let Some((player_index, touching_team, _)) = server.players.get_from_object_index(player)
        {
            if let Some(puck) = server.game.world.objects.get_puck_mut(puck_index) {
                add_touch(
                    puck,
                    self.puck_touches.entry(puck_index),
                    player_index,
                    player,
                    touching_team,
                    server.game.time,
                );

                let side = if puck.body.pos.x <= &server.game.world.rink.width / 2.0 {
                    HQMRinkSide::Left
                } else {
                    HQMRinkSide::Right
                };
                self.pass = Some(HQMPass {
                    team: touching_team,
                    side,
                    from: None,
                    player: player_index,
                });

                let other_team = touching_team.get_other_team();

                if let HQMOffsideStatus::Warning(team, side, position, i) = self.offside_status {
                    if team == touching_team {
                        let self_touch = player_index == i;

                        self.call_offside(server, touching_team, side, position, self_touch);
                        return;
                    }
                }
                if let HQMTwoLinePassStatus::Warning(team, side, position, ref i) =
                    self.twoline_pass_status
                {
                    if team == touching_team && i.contains(&player_index) {
                        self.call_twoline_pass(server, touching_team, side, position);
                        return;
                    } else {
                        self.twoline_pass_status = HQMTwoLinePassStatus::No;
                        server
                            .messages
                            .add_server_chat_message_str("Two-line pass waved off");
                    }
                }
                if let HQMIcingStatus::Warning(team, side) = self.icing_status {
                    if touching_team != team && !self.started_as_goalie.contains(&player_index) {
                        self.call_icing(server, other_team, side);
                    } else {
                        self.icing_status = HQMIcingStatus::No;
                        server
                            .messages
                            .add_server_chat_message_str("Icing waved off");
                    }
                }
            }
        }
    }

    fn handle_puck_entered_net(
        &mut self,
        server: &mut HQMServer,
        events: &mut Vec<HQMRankedEvent>,
        team: HQMTeam,
        puck: HQMObjectIndex,
    ) {
        match self.offside_status {
            HQMOffsideStatus::Warning(offside_team, side, position, _) if offside_team == team => {
                self.call_offside(server, team, side, position, false);
            }
            HQMOffsideStatus::Offside(_) => {}
            _ => {
                events.push(self.call_goal(server, team, puck));
            }
        }
    }

    fn handle_puck_passed_goal_line(&mut self, server: &mut HQMServer, team: HQMTeam) {
        if let Some(HQMPass {
            team: icing_team,
            side,
            from: Some(transition),
            ..
        }) = self.pass
        {
            if team == icing_team && transition <= HQMPassPosition::ReachedCenter {
                match self.config.icing {
                    HQMIcingConfiguration::Touch => {
                        self.icing_status = HQMIcingStatus::Warning(team, side);
                        server.messages.add_server_chat_message_str("Icing warning");
                    }
                    HQMIcingConfiguration::NoTouch => {
                        self.call_icing(server, team, side);
                    }
                    HQMIcingConfiguration::Off => {}
                }
            }
        }
    }

    fn puck_into_offside_zone(&mut self, server: &mut HQMServer, team: HQMTeam) {
        if self.offside_status == HQMOffsideStatus::InOffensiveZone(team) {
            return;
        }
        if let Some(HQMPass {
            team: pass_team,
            side,
            from: transition,
            player,
        }) = self.pass
        {
            if team == pass_team && has_players_in_offensive_zone(&server, team, Some(player)) {
                match self.config.offside {
                    HQMOffsideConfiguration::Delayed => {
                        self.offside_status =
                            HQMOffsideStatus::Warning(team, side, transition, player);
                        server
                            .messages
                            .add_server_chat_message_str("Offside warning");
                    }
                    HQMOffsideConfiguration::Immediate => {
                        self.call_offside(server, team, side, transition, false);
                    }
                    HQMOffsideConfiguration::Off => {
                        self.offside_status = HQMOffsideStatus::InOffensiveZone(team);
                    }
                }
            } else {
                self.offside_status = HQMOffsideStatus::InOffensiveZone(team);
            }
        } else {
            self.offside_status = HQMOffsideStatus::InOffensiveZone(team);
        }
    }

    fn handle_puck_entered_offensive_half(&mut self, server: &mut HQMServer, team: HQMTeam) {
        if !matches!(&self.offside_status, HQMOffsideStatus::Offside(_))
            && self.config.offside_line == HQMOffsideLineConfiguration::Center
        {
            self.puck_into_offside_zone(server, team);
        }
        if let HQMOffsideStatus::Warning(warning_team, _, _, _) = self.offside_status {
            if warning_team != team {
                server
                    .messages
                    .add_server_chat_message_str("Offside waved off");
            }
        }
        if let Some(HQMPass {
            team: pass_team,
            side,
            from: Some(from),
            player: pass_player,
        }) = self.pass
        {
            if self.twoline_pass_status == HQMTwoLinePassStatus::No && pass_team == team {
                let is_regular_twoline_pass_active = self.config.twoline_pass
                    == HQMTwoLinePassConfiguration::Double
                    || self.config.twoline_pass == HQMTwoLinePassConfiguration::On;
                if from <= HQMPassPosition::ReachedOwnBlue && is_regular_twoline_pass_active {
                    self.check_twoline_pass(server, team, side, from, pass_player, false);
                }
            }
        }
    }

    fn handle_puck_entered_offensive_zone(&mut self, server: &mut HQMServer, team: HQMTeam) {
        if !matches!(&self.offside_status, HQMOffsideStatus::Offside(_))
            && self.config.offside_line == HQMOffsideLineConfiguration::OffensiveBlue
        {
            self.puck_into_offside_zone(server, team);
        }
        if let Some(HQMPass {
            team: pass_team,
            side,
            from: Some(from),
            player: pass_player,
        }) = self.pass
        {
            if self.twoline_pass_status == HQMTwoLinePassStatus::No && pass_team == team {
                let is_forward_twoline_pass_active = self.config.twoline_pass
                    == HQMTwoLinePassConfiguration::Double
                    || self.config.twoline_pass == HQMTwoLinePassConfiguration::Forward;
                let is_threeline_pass_active =
                    self.config.twoline_pass == HQMTwoLinePassConfiguration::ThreeLine;
                if (from <= HQMPassPosition::ReachedCenter && is_forward_twoline_pass_active)
                    || from <= HQMPassPosition::ReachedOwnBlue && is_threeline_pass_active
                {
                    self.check_twoline_pass(server, team, side, from, pass_player, true);
                }
            }
        }
    }

    fn check_twoline_pass(
        &mut self,
        server: &mut HQMServer,
        team: HQMTeam,
        side: HQMRinkSide,
        from: HQMPassPosition,
        pass_player: HQMServerPlayerIndex,
        is_offensive_line: bool,
    ) {
        let team_line = match team {
            HQMTeam::Red => &server.game.world.rink.red_lines_and_net,
            HQMTeam::Blue => &server.game.world.rink.blue_lines_and_net,
        };
        let line = if is_offensive_line {
            &team_line.offensive_line
        } else {
            &team_line.mid_line
        };
        let mut players_past_line = vec![];
        for (player_index, player) in server.players.iter() {
            if player_index == pass_player {
                continue;
            }
            if let Some(player) = player {
                if is_past_line(server, player, team, line) {
                    players_past_line.push(player_index);
                }
            }
        }
        if !players_past_line.is_empty() {
            self.twoline_pass_status =
                HQMTwoLinePassStatus::Warning(team, side, from, players_past_line);
            server
                .messages
                .add_server_chat_message_str("Two-line pass warning");
        }
    }

    fn handle_puck_passed_defensive_line(&mut self, server: &mut HQMServer, team: HQMTeam) {
        if !matches!(&self.offside_status, HQMOffsideStatus::Offside(_))
            && self.config.offside_line == HQMOffsideLineConfiguration::OffensiveBlue
        {
            if let HQMOffsideStatus::Warning(t, _, _, _) = self.offside_status {
                if team.get_other_team() == t {
                    server
                        .messages
                        .add_server_chat_message_str("Offside waved off");
                }
            }
            self.offside_status = HQMOffsideStatus::Neutral;
        }
    }

    fn update_pass(&mut self, team: HQMTeam, p: HQMPassPosition) {
        if let Some(pass) = &mut self.pass {
            if pass.team == team && pass.from.is_none() {
                pass.from = Some(p);
            }
        }
    }

    fn check_wave_off_twoline(&mut self, server: &mut HQMServer, team: HQMTeam) {
        if let HQMTwoLinePassStatus::Warning(warning_team, _, _, _) = self.twoline_pass_status {
            if team != warning_team {
                self.twoline_pass_status = HQMTwoLinePassStatus::No;
                server
                    .messages
                    .add_server_chat_message_str("Two-line pass waved off");
            }
        }
    }

    fn handle_events(
        &mut self,
        server: &mut HQMServer,
        events: &[HQMSimulationEvent],
        match_events: &mut Vec<HQMRankedEvent>,
    ) {
        for event in events {
            match *event {
                HQMSimulationEvent::PuckEnteredNet { team, puck } => {
                    self.handle_puck_entered_net(server, match_events, team, puck);
                }
                HQMSimulationEvent::PuckTouch { player, puck, .. } => {
                    self.handle_puck_touch(server, player, puck);
                }
                HQMSimulationEvent::PuckReachedDefensiveLine { team, puck: _ } => {
                    self.check_wave_off_twoline(server, team);
                    self.update_pass(team, HQMPassPosition::ReachedOwnBlue);
                }
                HQMSimulationEvent::PuckPassedDefensiveLine { team, puck: _ } => {
                    self.update_pass(team, HQMPassPosition::PassedOwnBlue);
                    self.handle_puck_passed_defensive_line(server, team);
                }
                HQMSimulationEvent::PuckReachedCenterLine { team, puck: _ } => {
                    self.check_wave_off_twoline(server, team);
                    self.update_pass(team, HQMPassPosition::ReachedCenter);
                }
                HQMSimulationEvent::PuckPassedCenterLine { team, puck: _ } => {
                    self.update_pass(team, HQMPassPosition::PassedCenter);
                    self.handle_puck_entered_offensive_half(server, team);
                }
                HQMSimulationEvent::PuckReachedOffensiveZone { team, puck: _ } => {
                    self.update_pass(team, HQMPassPosition::ReachedOffensive);
                }
                HQMSimulationEvent::PuckEnteredOffensiveZone { team, puck: _ } => {
                    self.update_pass(team, HQMPassPosition::PassedOffensive);
                    self.handle_puck_entered_offensive_zone(server, team);
                }
                HQMSimulationEvent::PuckPassedGoalLine { team, puck: _ } => {
                    self.handle_puck_passed_goal_line(server, team);
                }
                _ => {}
            }

            if self.pause_timer > 0
                || server.game.time == 0
                || server.game.game_over
                || server.game.period == 0
            {
                return;
            }
        }
    }

    fn call_offside(
        &mut self,
        server: &mut HQMServer,
        team: HQMTeam,
        side: HQMRinkSide,
        position: Option<HQMPassPosition>,
        self_touch: bool,
    ) {
        let time_break = self.config.time_break * 100;

        let faceoff_spot = if self_touch {
            match self.config.offside_line {
                HQMOffsideLineConfiguration::OffensiveBlue => {
                    HQMRinkFaceoffSpot::Offside(team.get_other_team(), side)
                }
                HQMOffsideLineConfiguration::Center => HQMRinkFaceoffSpot::Center,
            }
        } else {
            match position {
                Some(p) if p <= HQMPassPosition::ReachedOwnBlue => {
                    HQMRinkFaceoffSpot::DefensiveZone(team, side)
                }
                Some(p) if p <= HQMPassPosition::ReachedCenter => {
                    HQMRinkFaceoffSpot::Offside(team, side)
                }
                _ => HQMRinkFaceoffSpot::Center,
            }
        };

        self.next_faceoff_spot = faceoff_spot;
        self.pause_timer = time_break;
        self.offside_status = HQMOffsideStatus::Offside(team);
        server.messages.add_server_chat_message_str("Offside");
    }

    fn call_twoline_pass(
        &mut self,
        server: &mut HQMServer,
        team: HQMTeam,
        side: HQMRinkSide,
        position: HQMPassPosition,
    ) {
        let time_break = self.config.time_break * 100;

        let faceoff_spot = if position <= HQMPassPosition::ReachedOwnBlue {
            HQMRinkFaceoffSpot::DefensiveZone(team, side)
        } else if position <= HQMPassPosition::ReachedCenter {
            HQMRinkFaceoffSpot::Offside(team, side)
        } else {
            HQMRinkFaceoffSpot::Center
        };

        self.next_faceoff_spot = faceoff_spot;
        self.pause_timer = time_break;
        self.twoline_pass_status = HQMTwoLinePassStatus::Offside(team);
        server.messages.add_server_chat_message_str("Two-line pass");
    }

    fn call_icing(&mut self, server: &mut HQMServer, team: HQMTeam, side: HQMRinkSide) {
        let time_break = self.config.time_break * 100;

        self.next_faceoff_spot = HQMRinkFaceoffSpot::DefensiveZone(team, side);
        self.pause_timer = time_break;
        self.icing_status = HQMIcingStatus::Icing(team);
        server.messages.add_server_chat_message_str("Icing");
    }

    pub fn main_tick(&mut self, server: &mut HQMServer) {
        self.tick += 1;

        if self.tick % 100 == 0 {
            let api = self.config.api.clone();
            let token = self.config.token.clone();
            let server_name = self.config.server_name.clone();
            let logged_in = self.queued_players.len();
            let team_max = self.config.team_max.clone();
            let period = server.game.period.clone();
            let time = server.game.time.clone();
            let red_score = server.game.red_score.clone();
            let blue_score = server.game.blue_score.clone();
            let mut state = 0;

            if let State::Waiting {
                waiting_for_response,
            } = self.status
            {
                state = 0;
            } else if let State::Game { paused } = self.status {
                state = 2;
            } else if let State::CaptainsPicking {
                ref mut time_left, ..
            } = self.status
            {
                state = 1;
            }

            let client = server.reqwest_client.clone();
            tokio::spawn(async move {
                let url = format!("{}/api/Server/Heartbeat", api);

                let request = HeartbeatRequest {
                    token: token,
                    name: server_name,
                    loggedIn: logged_in,
                    teamMax: team_max,
                    period: period,
                    time: time,
                    redScore: red_score,
                    blueScore: blue_score,
                    state: state,
                };

                let _response: reqwest::Response =
                    client.post(url).json(&request).send().await.unwrap();

                Ok::<_, anyhow::Error>(())
            });
        }
    }

    pub fn after_tick(
        &mut self,
        server: &mut HQMServer,
        events: &[HQMSimulationEvent],
    ) -> Vec<HQMRankedEvent> {
        let mut match_events = vec![];
        if server.game.time == 0 && server.game.period > 1 {
            self.handle_events_end_of_period(server, events);
        } else if self.pause_timer > 0
            || server.game.time == 0
            || server.game.game_over
            || server.game.period == 0
            || self.paused
        {
            // Nothing
        } else {
            self.handle_events(server, events, &mut match_events);

            if let HQMOffsideStatus::Warning(team, _, _, _) = self.offside_status {
                if !has_players_in_offensive_zone(server, team, None) {
                    self.offside_status = HQMOffsideStatus::InOffensiveZone(team);
                    server
                        .messages
                        .add_server_chat_message_str("Offside waved off");
                }
            }

            let rules_state = if matches!(self.offside_status, HQMOffsideStatus::Offside(_))
                || matches!(self.twoline_pass_status, HQMTwoLinePassStatus::Offside(_))
            {
                HQMRulesState::Offside
            } else if matches!(self.icing_status, HQMIcingStatus::Icing(_)) {
                HQMRulesState::Icing
            } else {
                let icing_warning = matches!(self.icing_status, HQMIcingStatus::Warning(_, _));
                let offside_warning =
                    matches!(self.offside_status, HQMOffsideStatus::Warning(_, _, _, _))
                        || matches!(
                            self.twoline_pass_status,
                            HQMTwoLinePassStatus::Warning(_, _, _, _)
                        );
                HQMRulesState::Regular {
                    offside_warning,
                    icing_warning,
                }
            };

            server.game.rules_state = rules_state;
        }

        self.update_clock(server);

        if let Some((start_replay, end_replay, force_view)) = self.start_next_replay {
            if end_replay <= server.game.game_step {
                server.add_replay_to_queue(start_replay, end_replay, force_view);
                server.messages.add_server_chat_message_str("Goal replay");
                self.start_next_replay = None;
            }
        }

        if server.game.vote_timer != 0 {
            server.game.vote_timer -= 1;

            if server.game.vote_timer == 0 {
                server
                    .messages
                    .add_server_chat_message_str("[Server] Vote canceled");
                server.game.vote = Vote::None;
            }
        }

        match_events
    }

    fn update_clock(&mut self, server: &mut HQMServer) {
        let period_length = self.config.time_period * 100;
        let intermission_time = self.config.time_intermission * 100;

        if !self.paused {
            if self.pause_timer > 0 {
                self.pause_timer -= 1;
                if self.pause_timer == 0 {
                    self.is_pause_goal = false;
                    if server.game.game_over {
                        server.new_game(self.create_game());
                    } else {
                        if server.game.time == 0 {
                            server.game.time = period_length;
                        }

                        self.do_faceoff(server);
                    }
                }
            } else {
                server.game.time = server.game.time.saturating_sub(1);
                if server.game.time == 0 {
                    server.game.period += 1;
                    self.pause_timer = intermission_time;
                    self.is_pause_goal = false;
                    self.step_where_period_ended = server.game.game_step;
                    self.too_late_printed_this_period = false;
                    self.next_faceoff_spot = HQMRinkFaceoffSpot::Center;
                    self.update_game_over(server);
                }
            }
        }
        server.game.goal_message_timer = if self.is_pause_goal {
            self.pause_timer
        } else {
            0
        };
    }

    pub fn cleanup_player(&mut self, player_index: HQMServerPlayerIndex) {
        if let Some(x) = self
            .started_as_goalie
            .iter()
            .position(|x| *x == player_index)
        {
            self.started_as_goalie.remove(x);
        }
        self.preferred_positions.remove(&player_index);
    }

    pub fn create_game(&mut self) -> HQMGame {
        self.status = State::Waiting {
            waiting_for_response: false,
        };
        self.puck_touches.clear();
        self.rhqm_game = RHQMGame::new();
        self.paused = true;
        self.pause_timer = 0;
        self.next_faceoff_spot = HQMRinkFaceoffSpot::Center;
        self.icing_status = HQMIcingStatus::No;
        self.offside_status = HQMOffsideStatus::Neutral;
        self.twoline_pass_status = HQMTwoLinePassStatus::No;
        self.start_next_replay = None;

        let warmup_pucks = self.config.warmup_pucks;

        let mut game = HQMGame::new(
            warmup_pucks,
            self.config.physics_config.clone(),
            self.config.blue_line_location,
        );
        let puck_line_start = game.world.rink.width / 2.0 - 0.4 * ((warmup_pucks - 1) as f32);

        for i in 0..warmup_pucks {
            let pos = Point3::new(
                puck_line_start + 0.8 * (i as f32),
                1.5,
                game.world.rink.length / 2.0,
            );
            let rot = Rotation3::identity();
            game.world.create_puck_object(pos, rot);
        }
        game.time = self.config.time_warmup * 100;
        game
    }

    pub fn onethree(&mut self, server: &mut HQMServer, player_index: HQMServerPlayerIndex) {
        if matches!(self.status, State::Game { .. }) {
            let player = server.players.get(player_index);
            let team = if let Some(team) = player.and_then(|x| x.object).map(|x| x.1) {
                team
            } else {
                return;
            };

            let mut players_in_team = SmallVec::<[HQMServerPlayerIndex; 8]>::new();

            for (player_index, player) in server.players.iter() {
                let player_team = player.and_then(|x| x.object).map(|x| x.1);
                if player_team == Some(team) {
                    players_in_team.push(player_index);
                }
            }

            let goalie = match team {
                HQMTeam::Red => &self.rhqm_game.red_goalie,
                HQMTeam::Blue => &self.rhqm_game.blue_goalie,
            };

            match goalie {
                RHQMGameGoalie::TakeTurns {
                    first,
                    second,
                    third,
                    overtime,
                } => {
                    let msg1 = format!("[Server] G 1st: {}, 2nd: {}", limit(first), limit(second));
                    let msg2 = format!("[Server] G 3rd: {}, OT: {}", limit(third), limit(overtime));
                    for player_index in players_in_team.iter() {
                        server
                            .messages
                            .add_directed_server_chat_message(msg1.clone(), *player_index);
                        server
                            .messages
                            .add_directed_server_chat_message(msg2.clone(), *player_index);
                    }
                }
                RHQMGameGoalie::Fixed { goalie } => {
                    let msg = format!("[Server] G: {}", goalie);
                    for player_index in players_in_team.iter() {
                        server
                            .messages
                            .add_directed_server_chat_message(msg.clone(), *player_index);
                    }
                }
                RHQMGameGoalie::Undefined => {}
            };
        }
    }

    pub fn iamg(&mut self, server: &mut HQMServer, player_index: HQMServerPlayerIndex) {
        if matches!(self.status, State::Game { .. }) {
            let player = self.rhqm_game.get_player_by_index(player_index);
            let team = if let Some(team) = player.and_then(|x| x.player_team) {
                team
            } else {
                return;
            };

            let mut players_in_team = SmallVec::<[HQMServerPlayerIndex; 8]>::new();

            for player_item in self.rhqm_game.game_players.iter() {
                if let Some(player_index) = player_item.player_index {
                    if Some(team) == player_item.player_team {
                        players_in_team.push(player_index);
                    }
                }
            }

            if let Some(player) = self.rhqm_game.get_player_by_index(player_index) {
                let name = player.player_name.clone();
                let msg = format!("[Server] {} will be goalie for this game!", name);
                let goalie = match team {
                    HQMTeam::Red => &mut self.rhqm_game.red_goalie,
                    HQMTeam::Blue => &mut self.rhqm_game.blue_goalie,
                };
                *goalie = RHQMGameGoalie::Fixed { goalie: name };

                for player_index in players_in_team.iter() {
                    server
                        .messages
                        .add_directed_server_chat_message(msg.clone(), *player_index);
                }
            }
        }
    }

    pub(crate) fn handle_responses(&mut self, server: &mut HQMServer) {
        let responses: Vec<ApiResponse> = self.receiver.try_iter().collect();
        for response in responses {
            match response {
                ApiResponse::LoginFailed {
                    player_index,
                    error_message,
                } => {
                    server
                        .messages
                        .add_directed_server_chat_message(error_message, player_index);
                }
                ApiResponse::LoginSuccessful {
                    player_id,
                    player_index,
                    old_nickname,
                } => {
                    self.successful_login(server, player_index, player_id, old_nickname);
                }
                ApiResponse::GameStarted {
                    gameId,
                    players,
                    captainRed,
                    captainBlue,
                } => {
                    server.game.game_id = gameId;
                    server.game_uuid = server.game.game_id.clone();

                    if !matches!(
                        self.status,
                        State::Waiting {
                            waiting_for_response: true
                        }
                    ) {
                        continue;
                    }
                    let mut successful = true;
                    let mut new_players = vec![];
                    let mut player_indices = vec![];
                    for p in players {
                        let queue_player = self.queued_players.iter().find(|q| q.player_id == p.id);
                        if let Some(queue_player) = queue_player {
                            let rhqm_player = RHQMGamePlayer {
                                player_id: p.id,
                                player_name: queue_player.player_name.clone(),
                                player_index: Some(queue_player.player_index),
                                player_team: None,
                                rating: p.score,
                            };
                            player_indices.push(queue_player.player_index);
                            new_players.push(rhqm_player);
                        } else {
                            successful = false;
                        }
                    }

                    if successful {
                        self.paused = false;
                        self.force_players_off_ice_by_system(server);
                        server.game.time = 2000;
                        self.rhqm_game.game_players = new_players;
                        self.queued_players
                            .retain(|x| !player_indices.contains(&x.player_index));
                        self.status = State::CaptainsPicking {
                            time_left: 2000,
                            options: self.set_captains(server, captainRed, captainBlue),
                            current_team: HQMTeam::Red,
                        };
                        self.send_available_picks(server);
                    } else {
                        self.status = State::Waiting {
                            waiting_for_response: false,
                        }
                    }
                }
                ApiResponse::GameEnded { mvp, players } => {
                    let msg = format!("[Server] MVP: {}", mvp);
                    server.messages.add_server_chat_message(msg);

                    for p in players {
                        let rhqm_player = self.rhqm_game.get_player_by_id(p.id.clone());
                        if let Some(rhqm_player) = rhqm_player {
                            if let Some(p_index) = rhqm_player.player_index {
                                let player_msg = format!(
                                    "[Server] Rating: {} ({}) #{}",
                                    p.total, p.score, p.pos
                                );
                                server
                                    .messages
                                    .add_directed_server_chat_message(player_msg, p_index);
                            }
                        }
                    }
                }
                ApiResponse::Report {
                    message,
                    success,
                    player_index,
                } => {
                    if success {
                        let msg = format!("{}", message);
                        server.messages.add_server_chat_message(msg);
                    } else {
                        let player_msg = format!("{}", message);
                        server
                            .messages
                            .add_directed_server_chat_message(player_msg, player_index);
                    }
                }
                ApiResponse::Error { error_message } => {
                    server.messages.add_server_chat_message(error_message);
                }
            }
        }
    }

    fn set_captains(
        &mut self,
        server: &mut HQMServer,
        red_cap: i32,
        blue_cap: i32,
    ) -> Vec<(String, String, i32, i32)> {
        let middle = server.game.world.rink.length / 2.0;

        self.rhqm_game
            .game_players
            .sort_by(|a, b| b.rating.cmp(&a.rating));

        let red_captain = &mut self.rhqm_game.game_players[1];
        let red_captain_name = red_captain.player_name.clone();
        red_captain.player_team = Some(HQMTeam::Red);
        self.rhqm_game.red_captain = Some(red_captain.player_id);
        if let Some(red_captain_index) = red_captain.player_index {
            let pos = Point3::new(0.5, 2.0, middle + 3.0);
            let rot = Rotation3::from_euler_angles(0.0, 3.0 * FRAC_PI_2, 0.0);
            server.spawn_skater(red_captain_index, HQMTeam::Red, pos, rot);
        }

        let blue_captain = &mut self.rhqm_game.game_players[0];
        let blue_captain_name = blue_captain.player_name.clone();
        blue_captain.player_team = Some(HQMTeam::Blue);
        self.rhqm_game.blue_captain = Some(blue_captain.player_id);
        if let Some(blue_captain_index) = blue_captain.player_index {
            let pos = Point3::new(0.5, 2.0, middle - 3.0);
            let rot = Rotation3::from_euler_angles(0.0, 3.0 * FRAC_PI_2, 0.0);
            server.spawn_skater(blue_captain_index, HQMTeam::Blue, pos, rot);
        }

        let mut options = vec![];

        for (player, c) in self.rhqm_game.game_players[2..]
            .iter()
            .zip("ABCDEFGHIJKLMNOPQRSTUVWXYZ".chars())
        {
            options.push((
                c.to_string(),
                player.player_name.clone(),
                player.player_id,
                player.rating,
            ));
        }

        let msg = format!(
            "[Server] Red ({}) vs Blue ({})",
            limit(&red_captain_name),
            limit(&blue_captain_name)
        );

        server.messages.add_server_chat_message(msg);

        return options;
    }

    fn successful_login(
        &mut self,
        server: &mut HQMServer,
        player_index: HQMServerPlayerIndex,
        player_id: i32,
        old_nickname: String,
    ) {
        if let Some(player) = server.players.get(player_index) {
            self.verified_players.insert(player_index, player_id);
            let rhqm_player = self.rhqm_game.get_player_by_id_mut(player_id.clone());
            let is_on_ice = player.object.is_some();
            let queued = self
                .queued_players
                .iter()
                .any(|x| &x.player_id == &player_id);
            if let Some(rhqm_player) = rhqm_player {
                if rhqm_player.player_index.is_some() {
                    server.messages.add_directed_server_chat_message_str(
                        "You are already logged in",
                        player_index,
                    );
                } else {
                    rhqm_player.player_index = Some(player_index);
                    if !is_on_ice && matches!(self.status, State::Game { .. }) {
                        if let Some(team) = rhqm_player.player_team {
                            server.spawn_skater_at_spawnpoint(
                                player_index,
                                team,
                                HQMSpawnPoint::Bench,
                            );
                        }
                    }
                }
            } else if queued {
                server.messages.add_directed_server_chat_message_str(
                    "You are already logged in",
                    player_index,
                );
            } else {
                let name = player.player_name.to_string();
                let player_item = RHQMQueuePlayer {
                    player_id,
                    player_index,
                    player_name: name.clone(),
                    afk: false,
                };
                self.queued_players.push(player_item);

                let mut old_nickname_text = String::from("");
                if old_nickname.len() != 0 {
                    old_nickname_text = format!(" ({})", old_nickname);
                }

                let msg = format!(
                    "[Server] {}{} logged in [{}/{}]",
                    name,
                    old_nickname_text,
                    self.queued_players.len().to_string(),
                    self.config.team_max * 2
                );

                server.messages.add_server_chat_message(msg);
                self.rhqm_game.need_to_send = true;
            }
        }
    }

    pub fn report(
        &mut self,
        server: &mut HQMServer,
        player_index: HQMServerPlayerIndex,
        reported_player_index: HQMServerPlayerIndex,
    ) {
        if let Some(reported_from) = self.rhqm_game.get_player_by_index(player_index) {
            let reported_from_rhqm_id = reported_from.player_id.clone();

            if let Some(reported_to) = self.rhqm_game.get_player_by_index(reported_player_index) {
                let reported_to_rhqm_id = reported_to.player_id.clone();

                let sender = self.sender.clone();
                let api = self.config.api.clone();
                let token = self.config.token.clone();
                let client = server.reqwest_client.clone();
                tokio::spawn(async move {
                    let url = format!("{}/api/Server/Report", api);

                    let request = ReportRequest {
                        token: token,
                        fromId: reported_from_rhqm_id,
                        toId: reported_to_rhqm_id,
                    };

                    let response: reqwest::Response =
                        client.post(url).json(&request).send().await.unwrap();

                    match response.status() {
                        reqwest::StatusCode::OK => {
                            match response.json::<ReportResponse>().await {
                                Ok(parsed) => {
                                    sender.send(ApiResponse::Report {
                                        message: parsed.message,
                                        success: parsed.success,
                                        player_index: player_index,
                                    })?;
                                }
                                Err(_) => sender.send(ApiResponse::Error {
                                    error_message: "[Server] Can't parse response".to_owned(),
                                })?,
                            };
                        }
                        _ => {
                            sender.send(ApiResponse::Error {
                                error_message: "[Server] Can't connect to host".to_owned(),
                            })?;
                        }
                    }

                    Ok::<_, anyhow::Error>(())
                });
            }
        }
    }

    pub fn login(
        &mut self,
        server: &mut HQMServer,
        player_index: HQMServerPlayerIndex,
        password_user: &str,
    ) {
        if let Some(player) = server.players.get(player_index) {
            let name = player.player_name.to_string();
            let rhqm_player = self.rhqm_game.get_player_by_index(player_index);
            let is_on_ice = player.object.is_some();
            let queued = self.queued_players.iter().any(|x| &x.player_name == &name);
            if queued || (rhqm_player.is_some() && is_on_ice) {
                server.messages.add_directed_server_chat_message_str(
                    "[Server] You are already logged in",
                    player_index,
                );
            } else {
                //login
                let sender = self.sender.clone();
                let username = (*player.player_name).clone();
                let pass = format!("{}", password_user);
                let api = self.config.api.clone();
                let token = self.config.token.clone();
                let client = server.reqwest_client.clone();
                tokio::spawn(async move {
                    let url = format!("{}/api/Server/Login", api);

                    let mut map = HashMap::new();
                    map.insert("login", &username);
                    map.insert("password", &pass);
                    map.insert("serverToken", &token);

                    let response: reqwest::Response =
                        client.post(url).json(&map).send().await.unwrap();

                    match response.status() {
                        reqwest::StatusCode::OK => {
                            match response.json::<APILoginResponse>().await {
                                Ok(parsed) => {
                                    if parsed.success {
                                        sender.send(ApiResponse::LoginSuccessful {
                                            player_id: parsed.id,
                                            player_index: player_index,
                                            old_nickname: parsed.oldNickname,
                                        })?;
                                    } else {
                                        sender.send(ApiResponse::LoginFailed {
                                            player_index: player_index,
                                            error_message: parsed.errorMessage,
                                        })?;
                                    }
                                }
                                Err(_) => {
                                    sender.send(ApiResponse::LoginFailed {
                                        player_index: player_index,
                                        error_message: "[Server] Can't parse response".to_owned(),
                                    })?;
                                }
                            };
                        }
                        _ => {
                            sender.send(ApiResponse::LoginFailed {
                                player_index: player_index,
                                error_message: "[Server] Can't connect to host".to_owned(),
                            })?;
                        }
                    }

                    Ok::<_, anyhow::Error>(())
                });
            }
        }
    }

    pub(crate) fn request_player_points_and_win_rate(
        &self,
        server: &mut HQMServer,
        player_ids: Vec<i32>,
    ) {
        let sender = self.sender.clone();
        let api = self.config.api.clone();
        let token = self.config.token.clone();
        let team_max: usize = self.config.team_max.clone();
        let client = server.reqwest_client.clone();
        tokio::spawn(async move {
            let url = format!("{}/api/Server/StartGame", api);

            let request = GameStartRequest {
                token: token,
                maxCount: team_max,
                playerIds: player_ids,
            };

            let response: reqwest::Response = client.post(url).json(&request).send().await.unwrap();

            match response.status() {
                reqwest::StatusCode::OK => {
                    match response.json::<APIGameStartResponse>().await {
                        Ok(parsed) => {
                            sender.send(ApiResponse::GameStarted {
                                gameId: parsed.gameId,
                                players: parsed.players,
                                captainRed: parsed.captainRed,
                                captainBlue: parsed.captainBlue,
                            })?;
                        }
                        Err(_) => sender.send(ApiResponse::Error {
                            error_message: "[Server] Can't parse response".to_owned(),
                        })?,
                    };
                }
                _ => {
                    sender.send(ApiResponse::Error {
                        error_message: "[Server] Can't connect to host".to_owned(),
                    })?;
                }
            }

            Ok::<_, anyhow::Error>(())
        });
    }

    fn force_players_off_ice_by_system(&mut self, server: &mut HQMServer) {
        for i in 0..server.players.len() {
            server.move_to_spectator(HQMServerPlayerIndex(i));
        }
    }

    pub fn pick(
        &mut self,
        server: &mut HQMServer,
        sending_player: HQMServerPlayerIndex,
        arg: &str,
    ) {
        if let State::CaptainsPicking {
            ref options,
            current_team,
            ..
        } = self.status
        {
            if let Some(picking_rhqm_player) = self.rhqm_game.get_player_by_index(sending_player) {
                let picking_rhqm_id = picking_rhqm_player.player_id.clone();

                let (current_picking_captain, other_picking_captain) = match current_team {
                    HQMTeam::Red => (
                        self.rhqm_game.red_captain.clone(),
                        self.rhqm_game.blue_captain.clone(),
                    ),
                    HQMTeam::Blue => (self.rhqm_game.blue_captain, self.rhqm_game.red_captain),
                };
                if current_picking_captain == Some(picking_rhqm_id) {
                    let p = options
                        .iter()
                        .find(|(a, _, _, _)| a.eq_ignore_ascii_case(arg))
                        .cloned();
                    if let Some((_, _, player_id, _)) = p {
                        self.make_pick(server, player_id);
                    } else {
                        server.messages.add_directed_server_chat_message_str(
                            "[Server] Invalid pick",
                            sending_player,
                        );
                    }
                } else if other_picking_captain == Some(picking_rhqm_id) {
                    server.messages.add_directed_server_chat_message_str(
                        "[Server] It's not your turn to pick",
                        sending_player,
                    );
                } else {
                    server.messages.add_directed_server_chat_message_str(
                        "[Server] You're not a captain",
                        sending_player,
                    );
                }
            }
        }
    }

    fn update_status_after_pick(&mut self, server: &mut HQMServer, player_id: i32) {
        if let State::CaptainsPicking {
            ref mut options,
            current_team,
            ..
        } = self.status
        {
            let pick_order = [HQMTeam::Blue, HQMTeam::Blue, HQMTeam::Red, HQMTeam::Red];
            let pick_order_len = pick_order.len();
            self.rhqm_game.pick_number = self.rhqm_game.pick_number + 1;
            let cu_pick = (self.rhqm_game.pick_number - 1) % pick_order_len;

            // Find out how many players there are left
            let remaining_players = self
                .rhqm_game
                .game_players
                .iter()
                .filter(|p| p.player_team.is_none())
                .map(|p| p.player_id)
                .collect::<Vec<_>>();
            if remaining_players.is_empty() {
                // Weird, should not happen
                self.start_captains_game(server);
            } else if remaining_players.len() == 1 {
                let other_team = pick_order[cu_pick];
                let remaining_player = self
                    .rhqm_game
                    .get_player_by_id_mut(remaining_players[0].clone())
                    .unwrap();
                remaining_player.player_team = Some(other_team);
                if let Some(player_index) = remaining_player.player_index {
                    server.spawn_skater_at_spawnpoint(
                        player_index,
                        other_team,
                        HQMSpawnPoint::Bench,
                    );
                }
                self.start_captains_game(server);
            } else {
                let mut old_options = std::mem::replace(options, vec![]);
                old_options.retain(|(_, _, id, _)| *id != player_id);
                server.game.period = 0;
                server.game.time = 2000;
                self.status = State::CaptainsPicking {
                    time_left: 2000,
                    options: old_options,
                    current_team: pick_order[cu_pick],
                };
                self.send_available_picks(server);
            }
        }
    }

    fn start_captains_game(&mut self, server: &mut HQMServer) {
        server.game.time = 2000;
        self.status = State::Game { paused: false };

        let mut red_player_names = SmallVec::<[_; 8]>::new();
        let mut blue_player_names = SmallVec::<[_; 8]>::new();
        for p in self.rhqm_game.game_players.iter() {
            match p.player_team {
                Some(HQMTeam::Red) => red_player_names.push(p.player_name.clone()),
                Some(HQMTeam::Blue) => blue_player_names.push(p.player_name.clone()),
                _ => {}
            };
        }
        let mut rand = rand::thread_rng();
        red_player_names.shuffle(&mut rand);
        blue_player_names.shuffle(&mut rand);
        let mut red_iter = red_player_names.into_iter().cycle();
        let mut blue_iter = blue_player_names.into_iter().cycle();
        self.rhqm_game.red_goalie = RHQMGameGoalie::TakeTurns {
            first: red_iter.next().unwrap(),
            second: red_iter.next().unwrap(),
            third: red_iter.next().unwrap(),
            overtime: red_iter.next().unwrap(),
        };
        self.rhqm_game.blue_goalie = RHQMGameGoalie::TakeTurns {
            first: blue_iter.next().unwrap(),
            second: blue_iter.next().unwrap(),
            third: blue_iter.next().unwrap(),
            overtime: blue_iter.next().unwrap(),
        };
    }

    pub(crate) fn make_default_pick(&mut self, server: &mut HQMServer) -> (String, i32) {
        if let State::CaptainsPicking { current_team, .. } = self.status {
            let best_remaining = self
                .rhqm_game
                .game_players
                .iter_mut()
                .filter(|p| p.player_team.is_none())
                .max_by_key(|x| x.rating)
                .unwrap();
            let player_id = best_remaining.player_id;
            let player_name = best_remaining.player_name.clone();
            best_remaining.player_team = Some(current_team);
            if let Some(player_index) = best_remaining.player_index {
                server.spawn_skater_at_spawnpoint(player_index, current_team, HQMSpawnPoint::Bench);
            }
            let msg = format!(
                "[Server] Time ran out, {} has been picked for {}",
                player_name, current_team
            );
            server.messages.add_server_chat_message(msg);

            self.update_status_after_pick(server, player_id);

            let api = self.config.api.clone();
            let token = self.config.token.clone();
            let game_id = server.game.game_id.clone();
            let p_id = player_id;
            let mut team = 0;
            if current_team == HQMTeam::Blue {
                team = 1;
            }
            let client = server.reqwest_client.clone();
            tokio::spawn(async move {
                let url = format!("{}/api/Server/Pick", api);

                let request = PickRequest {
                    token: token,
                    gameId: game_id,
                    playerId: p_id,
                    team: team,
                };

                let _response: reqwest::Response =
                    client.post(url).json(&request).send().await.unwrap();

                Ok::<_, anyhow::Error>(())
            });

            (player_name, player_id)
        } else {
            panic!();
        }
    }

    fn make_pick(&mut self, server: &mut HQMServer, player_id: i32) -> bool {
        if let State::CaptainsPicking { current_team, .. } = self.status {
            if let Some(picked_player) = self.rhqm_game.get_player_by_id_mut(player_id) {
                if picked_player.player_team.is_none() {
                    picked_player.player_team = Some(current_team);
                    let msg = format!(
                        "[Server] {} has been picked for {}",
                        picked_player.player_name, current_team
                    );
                    server.messages.add_server_chat_message(msg);
                    if let Some(player_index) = picked_player.player_index {
                        server.spawn_skater_at_spawnpoint(
                            player_index,
                            current_team,
                            HQMSpawnPoint::Bench,
                        );
                    }

                    let api = self.config.api.clone();
                    let token = self.config.token.clone();
                    let game_id = server.game.game_id.clone();
                    let p_id = picked_player.player_id;
                    let mut team = 0;
                    if current_team == HQMTeam::Blue {
                        team = 1;
                    }
                    let client = server.reqwest_client.clone();
                    tokio::spawn(async move {
                        let url = format!("{}/api/Server/Pick", api);

                        let request = PickRequest {
                            token: token,
                            gameId: game_id,
                            playerId: p_id,
                            team: team,
                        };

                        let _response: reqwest::Response =
                            client.post(url).json(&request).send().await.unwrap();

                        Ok::<_, anyhow::Error>(())
                    });

                    self.update_status_after_pick(server, player_id);
                    true
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        }
    }

    pub fn send_available_picks_command(
        &self,
        server: &mut HQMServer,
        player_index: HQMServerPlayerIndex,
    ) {
        if let State::CaptainsPicking { ref options, .. } = self.status {
            let mut msgs = SmallVec::<[_; 16]>::new();
            let iter = options.iter().chunks(3);
            for row in &iter {
                let s = row
                    .map(|(code, name, _, rating)| {
                        format!("{}: {} ({})", code, limit(name), rating)
                    })
                    .join(", ");
                msgs.push(s);
            }
            for msg in msgs.iter() {
                server
                    .messages
                    .add_directed_server_chat_message(msg.clone(), player_index);
            }
        }
    }

    pub fn send_resign(&self, server: &mut HQMServer, team: HQMTeam) {
        let api = self.config.api.clone();
        let token = self.config.token.clone();
        let game_id = server.game.game_id.clone();

        let mut resigned_team = 0;
        if team == HQMTeam::Blue {
            resigned_team = 1;
        }

        let client = server.reqwest_client.clone();

        server.game.game_over = true;

        tokio::spawn(async move {
            let url = format!("{}/api/Server/Resign", api);

            let request = ResignRequest {
                token: token,
                gameId: game_id,
                team: resigned_team,
            };

            let _response: reqwest::Response =
                client.post(url).json(&request).send().await.unwrap();

            Ok::<_, anyhow::Error>(())
        });
    }

    pub fn send_reset(&mut self, server: &mut HQMServer) {
        let api = self.config.api.clone();
        let token = self.config.token.clone();
        let game_id = server.game.game_id.clone();

        let client = server.reqwest_client.clone();

        let msg = format!("Game reset by vote");
        server.new_game(self.create_game());
        server.messages.add_server_chat_message(msg);

        tokio::spawn(async move {
            let url = format!("{}/api/Server/Reset", api);

            let request = ResetRequest {
                token: token,
                gameId: game_id,
            };

            let _response: reqwest::Response =
                client.post(url).json(&request).send().await.unwrap();

            Ok::<_, anyhow::Error>(())
        });
    }

    pub fn kick_by_vote(
        &mut self,
        server: &mut HQMServer,
        kick_player_index: HQMServerPlayerIndex,
    ) {
        if let Some(kick_player) = server.players.get(kick_player_index) {
            let kick_player_name = kick_player.player_name.clone();
            server.remove_player(kick_player_index, true);

            let msg = format!("{} kicked by vote", kick_player_name);
            server.messages.add_server_chat_message(msg);
        }
    }

    pub fn resign(&self, server: &mut HQMServer, player_index: HQMServerPlayerIndex) {
        if let Some(player) = self.rhqm_game.get_player_by_index(player_index) {
            let name = player.player_name.clone();

            let current_vote = server.game.vote.clone();
            if let Vote::Resign {
                mut player_indexes,
                team,
            } = current_vote
            {
                let player = server.players.get(player_index);
                let found_team = if let Some(team) = player.and_then(|x| x.object).map(|x| x.1) {
                    team
                } else {
                    return;
                };

                if found_team == team {
                    if player_indexes.iter().any(|&i| i == player_index) {
                        let msg = format!("[Server] You can vote once");
                        server
                            .messages
                            .add_directed_server_chat_message(msg, player_index);
                    } else {
                        player_indexes.push(player_index);
                        let count = player_indexes.len();
                        server.game.vote = Vote::Resign {
                            player_indexes: player_indexes,
                            team: team,
                        };

                        let msg = format!(
                            "[Server] {} voted for resign {}/{}",
                            name, count, self.config.team_max
                        );
                        server.messages.add_server_chat_message(msg);

                        if count == self.config.team_max - 1 {
                            self.send_resign(server, team);

                            server.game.vote = Vote::None;
                            server.game.vote_timer = 0;
                        }
                    }
                } else {
                    let msg = format!("[Server] Another team started vote");
                    server
                        .messages
                        .add_directed_server_chat_message(msg, player_index);
                }
            } else if let Vote::None {} = server.game.vote {
                let player = server.players.get(player_index);
                let team = if let Some(team) = player.and_then(|x| x.object).map(|x| x.1) {
                    team
                } else {
                    return;
                };
                server.game.vote = Vote::Resign {
                    player_indexes: [player_index].to_vec(),
                    team: team,
                };
                server.game.vote_timer = 2000;

                let msg = format!(
                    "[Server] {} voted for resign {}/{}",
                    name, 1, self.config.team_max
                );
                server.messages.add_server_chat_message(msg);
            } else {
                let msg = format!("[Server] Another vote started");
                server
                    .messages
                    .add_directed_server_chat_message(msg, player_index);
            }
        }
    }

    pub fn vote_reset(&mut self, server: &mut HQMServer, player_index: HQMServerPlayerIndex) {
        if let Some(player) = self.rhqm_game.get_player_by_index(player_index) {
            let name = player.player_name.clone();

            let current_vote = server.game.vote.clone();
            if let Vote::Reset { mut player_indexes } = current_vote {
                if player_indexes.iter().any(|&i| i == player_index) {
                    let msg = format!("[Server] You can vote once");
                    server
                        .messages
                        .add_directed_server_chat_message(msg, player_index);
                } else {
                    player_indexes.push(player_index);
                    let count = player_indexes.len();
                    server.game.vote = Vote::Reset {
                        player_indexes: player_indexes,
                    };

                    let msg = format!(
                        "[Server] {} voted for reset {}/{}",
                        name,
                        count,
                        self.config.team_max * 2 - 2
                    );
                    server.messages.add_server_chat_message(msg);

                    if count == self.config.team_max * 2 - 2 {
                        self.send_reset(server);
                        server.game.vote = Vote::None;
                        server.game.vote_timer = 0;
                    }
                }
            } else if let Vote::None {} = server.game.vote {
                server.game.vote = Vote::Reset {
                    player_indexes: [player_index].to_vec(),
                };
                server.game.vote_timer = 2000;

                let msg = format!(
                    "[Server] {} voted for reset {}/{}",
                    name,
                    1,
                    self.config.team_max * 2 - 2
                );
                server.messages.add_server_chat_message(msg);
            } else {
                let msg = format!("[Server] Another vote started");
                server
                    .messages
                    .add_directed_server_chat_message(msg, player_index);
            }
        }
    }

    pub fn vote_kick(
        &mut self,
        server: &mut HQMServer,
        player_index: HQMServerPlayerIndex,
        kick_player_index: HQMServerPlayerIndex,
    ) {
        if let Some(player) = self.rhqm_game.get_player_by_index(player_index) {
            let name = player.player_name.clone();

            if let Some(kick_player) = self.rhqm_game.get_player_by_index(kick_player_index) {
                let kick_player_name = kick_player.player_name.clone();
                let current_vote = server.game.vote.clone();
                if let Vote::Kick {
                    mut player_indexes,
                    kick,
                } = current_vote
                {
                    if player_indexes.iter().any(|&i| i == player_index) {
                        let msg = format!("[Server] You can vote once");
                        server
                            .messages
                            .add_directed_server_chat_message(msg, player_index);
                    } else {
                        player_indexes.push(player_index);
                        let count = player_indexes.len();
                        server.game.vote = Vote::Kick {
                            player_indexes: player_indexes,
                            kick: kick,
                        };

                        let msg = format!(
                            "[Server] {} voted for kick {} {}/{}",
                            name,
                            kick_player_name,
                            count,
                            self.config.team_max * 2 - 2
                        );
                        server.messages.add_server_chat_message(msg);

                        if count == self.config.team_max * 2 - 2 {
                            self.kick_by_vote(server, kick);
                            server.game.vote = Vote::None;
                            server.game.vote_timer = 0;
                        }
                    }
                } else if let Vote::None {} = server.game.vote {
                    server.game.vote = Vote::Kick {
                        player_indexes: [player_index].to_vec(),
                        kick: kick_player_index,
                    };
                    server.game.vote_timer = 2000;

                    let msg = format!(
                        "[Server] {} voted for kick {} {}/{}",
                        name,
                        kick_player_name,
                        1,
                        self.config.team_max * 2 - 2
                    );
                    server.messages.add_server_chat_message(msg);
                } else {
                    let msg = format!("[Server] Another vote started");
                    server
                        .messages
                        .add_directed_server_chat_message(msg, player_index);
                }
            }
        }
    }

    pub fn vote_mute(&self, server: &mut HQMServer, player_index: HQMServerPlayerIndex) {
        //todo
    }

    pub fn send_help(&self, server: &mut HQMServer, player_index: HQMServerPlayerIndex) {
        server
            .messages
            .add_directed_server_chat_message_str("/report # - report player", player_index);
        server
            .messages
            .add_directed_server_chat_message_str("/resign or /rs - vote to resign", player_index);
        server.messages.add_directed_server_chat_message_str(
            "/votereset or /vr - vote to cancel game",
            player_index,
        );
        server.messages.add_directed_server_chat_message_str(
            "/votekick # or /vk # - vote to kick player",
            player_index,
        );
    }

    fn send_available_picks(&self, server: &mut HQMServer) {
        if let State::CaptainsPicking {
            time_left: _,
            ref options,
            current_team,
        } = self.status
        {
            let current_captain = match current_team {
                HQMTeam::Red => self.rhqm_game.red_captain.clone(),
                HQMTeam::Blue => self.rhqm_game.blue_captain.clone(),
            }
            .and_then(|current_captain| self.rhqm_game.get_player_by_id(current_captain))
            .expect("captain value is invalid");

            let captain_name = current_captain.player_name.clone();
            let mut msgs = SmallVec::<[_; 16]>::new();
            let iter = options.iter().chunks(3);
            for row in &iter {
                let s = row
                    .map(|(code, name, _, rating)| {
                        format!("{}: {} ({})", code, limit(name), rating)
                    })
                    .join(", ");
                msgs.push(s);
            }
            let captain_player_index = current_captain.player_index;
            if let Some(captain_player_index) = captain_player_index {
                server.messages.add_directed_server_chat_message_str(
                    "It's your turn to pick! /p X",
                    captain_player_index,
                );
                for msg in msgs.iter() {
                    server
                        .messages
                        .add_directed_server_chat_message(msg.clone(), captain_player_index);
                }
            }
            let other_player_indices = server
                .players
                .iter()
                .filter(|(a, b)| b.is_some() && Some(*a) != captain_player_index)
                .map(|(a, _)| a)
                .collect::<Vec<_>>();
            for player_index in other_player_indices {
                let msg1 = format!(
                    "It's {}'s turn to pick for team {}!",
                    limit(&captain_name),
                    current_team
                );
                server
                    .messages
                    .add_directed_server_chat_message(msg1, player_index);

                for msg in msgs.iter() {
                    server
                        .messages
                        .add_directed_server_chat_message(msg.clone(), player_index);
                }
            }
        }
    }

    pub(crate) fn fix_standins(&mut self, server: &mut HQMServer) {
        self.rhqm_game.rejoin_timer.retain(|_, v| {
            *v = v.saturating_sub(1);
            *v > 0
        });

        let mut red_count = 0;
        let mut blue_count = 0;
        let mut red_actual_players = 0;
        let mut blue_actual_players = 0;
        let mut move_to_spectators = SmallVec::<[_; 8]>::new();
        let mut red_real_player_want_to_join = SmallVec::<[_; 8]>::new();
        let mut blue_real_player_want_to_join = SmallVec::<[_; 8]>::new();
        let mut red_standin_want_to_join = SmallVec::<[_; 8]>::new();
        let mut blue_standin_want_to_join = SmallVec::<[_; 8]>::new();
        let mut red_standins = SmallVec::<[_; 8]>::new();
        let mut blue_standins = SmallVec::<[_; 8]>::new();

        for rhqm_player in self.rhqm_game.game_players.iter() {
            match rhqm_player.player_team {
                Some(HQMTeam::Red) => {
                    red_actual_players += 1;
                }
                Some(HQMTeam::Blue) => {
                    blue_actual_players += 1;
                }
                _ => {}
            };
        }
        for (player_index, player) in server.players.iter() {
            if let Some(player) = player {
                let rhqm_player = self.rhqm_game.get_player_by_index(player_index);
                if let Some((_, team)) = player.object {
                    if player.input.spectate() {
                        self.rhqm_game.rejoin_timer.insert(player_index, 500);
                        move_to_spectators.push(player_index);
                    } else {
                        match team {
                            HQMTeam::Red => {
                                red_count += 1;
                            }
                            HQMTeam::Blue => {
                                blue_count += 1;
                            }
                        }
                        if rhqm_player.is_none() {
                            match team {
                                HQMTeam::Red => {
                                    red_standins.push(player_index);
                                }
                                HQMTeam::Blue => {
                                    blue_standins.push(player_index);
                                }
                            }
                        }
                    }
                } else {
                    if !self.rhqm_game.rejoin_timer.contains_key(&player_index) {
                        if let Some(rhqm_player) = rhqm_player {
                            if let Some(team) = rhqm_player.player_team {
                                if team == HQMTeam::Red && player.input.join_red() {
                                    red_real_player_want_to_join.push(player_index);
                                } else if team == HQMTeam::Blue && player.input.join_blue() {
                                    blue_real_player_want_to_join.push(player_index);
                                }
                            }
                        } else if player.input.join_red() {
                            if self
                                .queued_players
                                .iter()
                                .any(|x| x.player_index == player_index)
                            {
                                red_standin_want_to_join.push(player_index);
                            }
                        } else if player.input.join_blue() {
                            if self
                                .queued_players
                                .iter()
                                .any(|x| x.player_index == player_index)
                            {
                                blue_standin_want_to_join.push(player_index);
                            }
                        }
                    }
                }
            }
        }
        for player_index in move_to_spectators {
            server.move_to_spectator(player_index);
        }
        for player_index in red_real_player_want_to_join {
            red_count += 1;
            server.spawn_skater_at_spawnpoint(player_index, HQMTeam::Red, HQMSpawnPoint::Bench);
        }
        for player_index in blue_real_player_want_to_join {
            blue_count += 1;
            server.spawn_skater_at_spawnpoint(player_index, HQMTeam::Blue, HQMSpawnPoint::Bench);
        }
        if red_count < red_actual_players {
            for player_index in red_standin_want_to_join
                .into_iter()
                .take(red_actual_players - red_count)
            {
                red_count += 1;
                server.spawn_skater_at_spawnpoint(player_index, HQMTeam::Red, HQMSpawnPoint::Bench);
            }
        } else if red_count > red_actual_players {
            for player_index in red_standins
                .into_iter()
                .take(red_count - red_actual_players)
            {
                red_count -= 1;
                server.move_to_spectator(player_index);
            }
        }
        if blue_count < blue_actual_players {
            for player_index in blue_standin_want_to_join
                .into_iter()
                .take(blue_actual_players - blue_count)
            {
                blue_count += 1;
                server.spawn_skater_at_spawnpoint(
                    player_index,
                    HQMTeam::Blue,
                    HQMSpawnPoint::Bench,
                );
            }
        } else if blue_count > blue_actual_players {
            for player_index in blue_standins
                .into_iter()
                .take(blue_count - blue_actual_players)
            {
                blue_count -= 1;
                server.move_to_spectator(player_index);
            }
        }
    }

    pub(crate) fn save_data(&mut self, server: &mut HQMServer) {
        let api = self.config.api.clone();
        let token = self.config.token.clone();
        let game_id = server.game.game_id.clone();
        let client = server.reqwest_client.clone();
        let sender = self.sender.clone();
        tokio::spawn(async move {
            let url = format!("{}/api/Server/SaveGame", api);

            let request = SaveGameRequest {
                token: token,
                gameId: game_id,
            };

            let response: reqwest::Response = client.post(url).json(&request).send().await.unwrap();

            match response.status() {
                reqwest::StatusCode::OK => {
                    match response.json::<SaveGameResponse>().await {
                        Ok(parsed) => {
                            sender.send(ApiResponse::GameEnded {
                                mvp: parsed.mvp,
                                players: parsed.players,
                            })?;
                        }
                        Err(_) => sender.send(ApiResponse::Error {
                            error_message: "[Server] Can't parse response".to_owned(),
                        })?,
                    };
                }
                _ => {
                    sender.send(ApiResponse::Error {
                        error_message: "[Server] Can't connect to host".to_owned(),
                    })?;
                }
            }

            Ok::<_, anyhow::Error>(())
        });
    }

    pub(crate) fn get_player_and_ips(
        &self,
        server: &HQMServer,
        players: &[RHQMQueuePlayer],
    ) -> (Vec<String>, Vec<String>, Vec<i32>) {
        players
            .iter()
            .map(|x| {
                let name = x.player_name.clone();
                let ip = server
                    .players
                    .get(x.player_index)
                    .and_then(|player| player.addr().map(|x| x.ip()));
                let ip = ip.map(|x| x.to_string()).unwrap_or("".to_owned());

                (name, ip, x.player_id)
            })
            .multiunzip()
    }

    pub fn update_players(&mut self, server: &mut HQMServer) {
        self.handle_responses(server);

        if let State::Waiting {
            waiting_for_response,
        } = self.status
        {
            let mut players_to_spawn = smallvec::SmallVec::<[HQMServerPlayerIndex; 16]>::new();
            for queue_player in self.queued_players.iter() {
                if let Some(player) = server.players.get(queue_player.player_index) {
                    if player.object.is_none() {
                        players_to_spawn.push(queue_player.player_index);
                    }
                }
            }
            let middle = server.game.world.rink.length / 2.0;
            for (i, player_index) in players_to_spawn.into_iter().enumerate() {
                let pos = Point3::new(0.5, 2.0, middle - 10.0 + 2.0 * (i as f32));
                let rot = Rotation3::from_euler_angles(0.0, 3.0 * FRAC_PI_2, 0.0);
                server.spawn_skater(player_index, HQMTeam::Red, pos, rot);
            }
            if !waiting_for_response && self.queued_players.len() >= self.config.team_max * 2 {
                if self.delay_timer != 0 {
                    self.delay_timer -= 1;
                } else {
                    let slice = &self.queued_players[0..(self.config.team_max * 2)];
                    let (players, ips, player_ids) = self.get_player_and_ips(server, slice);

                    self.status = State::Waiting {
                        waiting_for_response: true,
                    };
                    self.request_player_points_and_win_rate(server, player_ids);
                }
            } else {
                self.delay_timer = self.config.delay * 100;
            }
        } else if let State::Game { paused } = self.status {
            if server.game.time % 1000 == 0 && !paused {
                if let Some(puck) = server.game.world.objects.get_puck(HQMObjectIndex(0)) {
                    self.rhqm_game.xpoints.push(puck.body.pos.x);
                    self.rhqm_game.zpoints.push(puck.body.pos.z);
                }
            }

            if server.game.game_over {
                if !self.rhqm_game.data_saved {
                    self.rhqm_game.data_saved = true;
                    self.save_data(server);
                }
            }

            self.fix_standins(server);
        } else if let State::CaptainsPicking {
            ref mut time_left, ..
        } = self.status
        {
            *time_left -= 1;
            if *time_left == 0 {
                self.make_default_pick(server);
            }
        }

        self.rhqm_game.notify_timer += 1;
    }
}

#[derive(Eq, PartialEq, Debug, Copy, Clone)]
pub enum HQMIcingConfiguration {
    Off,
    Touch,
    NoTouch,
}

#[derive(Eq, PartialEq, Debug, Copy, Clone)]
pub enum HQMOffsideConfiguration {
    Off,
    Delayed,
    Immediate,
}

#[derive(Eq, PartialEq, Debug, Copy, Clone)]
pub enum HQMTwoLinePassConfiguration {
    Off,
    On,
    Forward,
    Double,
    ThreeLine,
}

#[derive(Eq, PartialEq, Debug, Copy, Clone)]
pub enum HQMOffsideLineConfiguration {
    OffensiveBlue,
    Center,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum HQMPassPosition {
    None,
    ReachedOwnBlue,
    PassedOwnBlue,
    ReachedCenter,
    PassedCenter,
    ReachedOffensive,
    PassedOffensive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HQMPass {
    pub team: HQMTeam,
    pub side: HQMRinkSide,
    pub from: Option<HQMPassPosition>,
    pub player: HQMServerPlayerIndex,
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub enum HQMIcingStatus {
    No,                            // No icing
    Warning(HQMTeam, HQMRinkSide), // Puck has reached the goal line, delayed icing
    Icing(HQMTeam),                // Icing has been called
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub enum HQMOffsideStatus {
    Neutral,                  // No offside
    InOffensiveZone(HQMTeam), // No offside, puck in offensive zone
    Warning(
        HQMTeam,
        HQMRinkSide,
        Option<HQMPassPosition>,
        HQMServerPlayerIndex,
    ), // Warning, puck entered offensive zone in an offside situation but not touched yet
    Offside(HQMTeam),         // Offside has been called
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub enum HQMTwoLinePassStatus {
    No, // No offside
    Warning(
        HQMTeam,
        HQMRinkSide,
        HQMPassPosition,
        Vec<HQMServerPlayerIndex>,
    ), // Warning, puck entered offensive zone in an offside situation but not touched yet
    Offside(HQMTeam), // Offside has been called
}

#[derive(Debug, Clone)]
pub struct HQMPuckTouch {
    pub player_index: HQMServerPlayerIndex,
    pub skater_index: HQMObjectIndex,
    pub team: HQMTeam,
    pub puck_pos: Point3<f32>,
    pub puck_speed: f32,
    pub first_time: u32,
    pub last_time: u32,
}

pub fn add_touch(
    puck: &HQMPuck,
    entry: Entry<HQMObjectIndex, VecDeque<HQMPuckTouch>>,
    player_index: HQMServerPlayerIndex,
    skater_index: HQMObjectIndex,
    team: HQMTeam,
    time: u32,
) {
    let puck_pos = puck.body.pos.clone();
    let puck_speed = puck.body.linear_velocity.norm();

    let touches = entry.or_insert_with(|| VecDeque::new());
    let most_recent_touch = touches.front_mut();

    match most_recent_touch {
        Some(most_recent_touch)
            if most_recent_touch.player_index == player_index && most_recent_touch.team == team =>
        {
            most_recent_touch.puck_pos = puck_pos;
            most_recent_touch.last_time = time;
            most_recent_touch.puck_speed = puck_speed;
        }
        _ => {
            touches.truncate(15);
            touches.push_front(HQMPuckTouch {
                player_index,
                skater_index,
                team,
                puck_pos,
                puck_speed,
                first_time: time,
                last_time: time,
            });
        }
    }
}

pub fn get_faceoff_positions(
    players: &HQMServerPlayerList,
    preferred_positions: &HashMap<HQMServerPlayerIndex, String>,
    world: &HQMGameWorld,
) -> HashMap<HQMServerPlayerIndex, (HQMTeam, String)> {
    let allowed_positions = &world.rink.allowed_positions;
    let mut res = HashMap::new();

    let mut red_players = smallvec::SmallVec::<[_; 32]>::new();
    let mut blue_players = smallvec::SmallVec::<[_; 32]>::new();
    for (player_index, player) in players.iter() {
        if let Some(player) = player {
            let team = player.object.map(|x| x.1);

            let preferred_position = preferred_positions.get(&player_index).map(String::as_str);

            if team == Some(HQMTeam::Red) {
                red_players.push((player_index, preferred_position));
            } else if team == Some(HQMTeam::Blue) {
                blue_players.push((player_index, preferred_position));
            }
        }
    }

    setup_position(&mut res, &red_players, allowed_positions, HQMTeam::Red);
    setup_position(&mut res, &blue_players, allowed_positions, HQMTeam::Blue);

    res
}

pub fn is_past_line(
    server: &HQMServer,
    player: &HQMServerPlayer,
    team: HQMTeam,
    line: &HQMRinkLine,
) -> bool {
    if let Some((object_index, skater_team)) = player.object {
        if skater_team == team {
            if let Some(skater) = server.game.world.objects.get_skater(object_index) {
                let feet_pos =
                    &skater.body.pos - (&skater.body.rot * Vector3::y().scale(skater.height));
                let dot = (&feet_pos - &line.point).dot(&line.normal);
                let leading_edge = -(line.width / 2.0);
                if dot < leading_edge {
                    // Player is past line
                    return true;
                }
            }
        }
    }
    false
}

pub fn has_players_in_offensive_zone(
    server: &HQMServer,
    team: HQMTeam,
    ignore_player: Option<HQMServerPlayerIndex>,
) -> bool {
    let line = match team {
        HQMTeam::Red => &server.game.world.rink.red_lines_and_net.offensive_line,
        HQMTeam::Blue => &server.game.world.rink.blue_lines_and_net.offensive_line,
    };

    for (player_index, player) in server.players.iter() {
        if Some(player_index) == ignore_player {
            continue;
        }
        if let Some(player) = player {
            if is_past_line(server, player, team, line) {
                return true;
            }
        }
    }

    false
}

fn setup_position(
    positions: &mut HashMap<HQMServerPlayerIndex, (HQMTeam, String)>,
    players: &[(HQMServerPlayerIndex, Option<&str>)],
    allowed_positions: &[String],
    team: HQMTeam,
) {
    let mut available_positions = Vec::from(allowed_positions);

    // First, we try to give each player its preferred position
    for (player_index, player_position) in players.iter() {
        if let Some(player_position) = player_position {
            if let Some(x) = available_positions
                .iter()
                .position(|x| x == *player_position)
            {
                let s = available_positions.remove(x);
                positions.insert(*player_index, (team, s));
            }
        }
    }

    // Some players did not get their preferred positions because they didn't have one,
    // or because it was already taken
    for (player_index, player_position) in players.iter() {
        if !positions.contains_key(player_index) {
            let s = if let Some(x) = available_positions.iter().position(|x| x == "C") {
                // Someone needs to be C
                let x = available_positions.remove(x);
                (team, x)
            } else if !available_positions.is_empty() {
                // Give out the remaining positions
                let x = available_positions.remove(0);
                (team, x)
            } else {
                // Oh no, we're out of legal starting positions
                if let Some(player_position) = player_position {
                    (team, (*player_position).to_owned())
                } else {
                    (team, "C".to_owned())
                }
            };
            positions.insert(*player_index, s);
        }
    }

    if let Some(x) = available_positions.iter().position(|x| x == "C") {
        let mut change_index = None;
        for (player_index, _) in players.iter() {
            if change_index.is_none() {
                change_index = Some(player_index);
            }

            if let Some((_, pos)) = positions.get(player_index) {
                if pos != "G" {
                    change_index = Some(player_index);
                    break;
                }
            }
        }

        if let Some(change_index) = change_index {
            let c = available_positions.remove(x);
            positions.insert(*change_index, (team, c));
        }
    }
}
