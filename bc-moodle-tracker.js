// Brightcove Player plugin - Moodle Viewer Tracker
// Le o parametro viewerId da URL do iframe e identifica o usuario no Analytics
videojs.getPlayer().ready(function() {
  var player = this;
    var params = new URLSearchParams(window.location.search);
      var viewerId = params.get('viewerId');
        if (viewerId) {
            player.bcAnalytics.client.setUser(viewerId);
              }
              });
              
