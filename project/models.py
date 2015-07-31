from project import db


class Channel(db.Model):

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(80))

    channel_no_airtel = db.Column(db.String(6))
    channel_no_tata_sky = db.Column(db.String(6))
    channel_no_cable = db.Column(db.String(6))

    channel_id_airtel = db.Column(db.String(40))
    channel_id_tata_sky = db.Column(db.String(40))
    channel_id_cable = db.Column(db.String(40))

    def __init__(self, name, source, channel_no, channel_id):
        self.name = name
        if source == 'airtel':
            self.channel_no_airtel = channel_no
            self.channel_id_airtel = channel_id
        elif source == 'tata_sky':
            self.channel_no_tata_sky = channel_no
            self.channel_id_tata_sky = channel_id
        else:
            self.channel_no_cable = channel_no
            self.channel_id_cable = channel_id

    def __repr__(self):
        return '<Channel %r>' % self.name


class Programme(db.Model):

    id = db.Column(db.BigInteger, primary_key=True)
    title = db.Column(db.String(100))
    desc = db.Column(db.Text)
    category = db.Column(db.String(100))
    ms_progid = db.Column(db.String(40))

    def __init__(self, title, desc, category, progid):
        self.title = title
        self.desc = desc
        self.category = category
        self.ms_progid = progid

    def __repr__(self):
        return '<Programme %r>' % self.title
