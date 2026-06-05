// Minimal MongoDB RBAC bootstrap for Trusted Zone.
// Existing root/admin user remains unchanged for maintenance.

const trustedDbName = process.env.TRUSTED_SEMISTRUCTURED_MONGO_DB || "trusted_zone_semi-structured";
const trustedUser = process.env.MONGODB_TRUSTED_USER || "trusted_semistructured_writer";
const trustedPassword = process.env.MONGODB_TRUSTED_PASSWORD || "trusted_semistructured_writer_password";

const trustedDb = db.getSiblingDB(trustedDbName);

if (!trustedDb.getUser(trustedUser)) {
  trustedDb.createUser({
    user: trustedUser,
    pwd: trustedPassword,
    roles: [{ role: "readWrite", db: trustedDbName }],
  });
  print(`Created MongoDB trusted service user ${trustedUser} on ${trustedDbName}`);
} else {
  trustedDb.updateUser(trustedUser, {
    pwd: trustedPassword,
    roles: [{ role: "readWrite", db: trustedDbName }],
  });
  print(`Updated MongoDB trusted service user ${trustedUser} on ${trustedDbName}`);
}
